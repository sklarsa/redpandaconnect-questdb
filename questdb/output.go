package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	loFieldBatching = "batching"
)

type timestampUnit string

const (
	nanos   timestampUnit = "nanos"
	micros  timestampUnit = "micros"
	millis  timestampUnit = "millis"
	seconds timestampUnit = "seconds"
	auto    timestampUnit = "auto"
)

func guessTimestampUnits(timestamp int64) timestampUnit {

	if timestamp < 10000000000 {
		return seconds
	} else if timestamp < 10000000000000 { // 11/20/2286, 5:46:40 PM in millis and 4/26/1970, 5:46:40 PM in micros
		return millis
	} else if timestamp < 10000000000000000 {
		return micros
	} else {
		return nanos
	}
}

func (t timestampUnit) From(value int64) time.Time {
	switch t {
	case nanos:
		return time.Unix(0, value).UTC()
	case micros:
		return time.UnixMicro(value).UTC()
	case millis:
		return time.UnixMilli(value).UTC()
	case seconds:
		return time.Unix(value, 0).UTC()
	case auto:
		return guessTimestampUnits(value).From(value).UTC()
	default:
		panic("unsupported timestampUnit: " + t)
	}
}

func questdbOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Pushes messages a QuestDB table").
		Description(`Todo: fill this in`+service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(loFieldBatching),
			service.NewStringField("client_conf_string").
				Description("QuestDB client configuration string").
				Secret().
				Example("http::addr=localhost:9000;"),
			service.NewStringField("table").
				Description("Destination table").
				Example("trades"),
			service.NewBoolField("skipUnsupportedTypes").
				Description("Skips unsupported types (logging them at the warning level)").
				Default(false),
			service.NewStringField("designatedTimestampField").
				Description("Name of the designated timestamp field").
				Optional(),
			service.NewStringField("designatedTimestampUnits").
				Description("Designated timestamp field units").
				Default("auto").
				LintRule(`root = if ["nanos","micros","millis","seconds","auto"].contains(this) != true { [ "valid options are \"nanos\", \"micros\", \"millis\", \"seconds\", \"auto\"" ] }`).
				Optional(),
			service.NewStringListField("timestampStringFields").
				Description("String fields with textual timestamps").
				Optional(),
			service.NewStringField("timestampStringFormat").
				Description("Timestamp format, used when parsing timestamp string fields. Specified in golang's time.Parse layout").
				Default(time.StampMicro+"Z0700").
				Optional(),
			service.NewStringListField("symbols").
				Description("Columns that should be the SYMBOL type (string values default to STRING)").
				Optional(),
		)
}

type questdbWriter struct {
	log *service.Logger

	pool *qdb.LineSenderPool

	skipUnsupportedTypes     bool
	symbols                  map[string]bool
	table                    string
	designatedTimestampField string
	designatedTimestampUnits timestampUnit
	timestampStringFormat    string
	timestampStringFields    map[string]bool
}

func fromConf(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
	w := &questdbWriter{
		log:                   mgr.Logger(),
		symbols:               map[string]bool{},
		timestampStringFields: map[string]bool{},
	}
	out = w

	if batchPol, err = conf.FieldBatchPolicy(loFieldBatching); err != nil {
		return
	}

	if mif, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	clientConfStr, err := conf.FieldString("client_conf_string")
	if err != nil {
		return
	}
	w.pool = qdb.PoolFromConf(clientConfStr, qdb.WithMaxSenders(mif))

	if w.table, err = conf.FieldString("table"); err != nil {
		return
	}

	symbols := []string{}
	if symbols, err = conf.FieldStringList("symbols"); err != nil {
		return
	}
	for _, s := range symbols {
		w.symbols[s] = true
	}

	timestampStringFields := []string{}
	if timestampStringFields, err = conf.FieldStringList("timestampStringFields"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}

	}
	for _, f := range timestampStringFields {
		w.timestampStringFields[f] = true
	}

	if w.designatedTimestampField, err = conf.FieldString("designatedTimestampField"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}

	var designatedTimestampUnits string
	if designatedTimestampUnits, err = conf.FieldString("designatedTimestampUnits"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}
	w.designatedTimestampUnits = timestampUnit(designatedTimestampUnits)

	if w.skipUnsupportedTypes, err = conf.FieldBool("skipUnsupportedTypes"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}

	if w.timestampStringFormat, err = conf.FieldString("timestampStringFormat"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}

	return
}

func (q *questdbWriter) Connect(ctx context.Context) error {
	return nil
}

func (q *questdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	sender, err := q.pool.Acquire(ctx)
	if err != nil {
		q.log.Errorf("QuestDB error: %v\n", err)
		return err
	}

	defer func() {
		err := q.pool.Release(ctx, sender)
		if err != nil {
			q.log.Errorf("QuestDB error: %v\n", err)
		}
	}()

	for i, msg := range batch {
		q.log.Tracef("Writing message %v", i)

		var designatedTimestamp time.Time

		sender.Table(q.table)

		fields := map[string]any{}

		if err := walkForFields(msg, fields); err != nil {
			err = fmt.Errorf("failed to walk JSON object: %v", err)
			q.log.Errorf("QuestDB error: %w", err)
			return err
		}

		for k, v := range fields {

			// Check to see if the field is a symbol
			if _, found := q.symbols[k]; found {
				sender.Symbol(k, fmt.Sprintf("%v", v))
				continue
			}

			// If field is designated timestamp, process it separately
			if k == q.designatedTimestampField {
				switch val := v.(type) {
				case time.Time:
					designatedTimestamp = val
				case string:
					t, err := time.Parse(q.timestampStringFormat, val)
					if err != nil {
						return fmt.Errorf("QuestDB error: could not parse designated timestamp field %w", err)
					}
					designatedTimestamp = t
				case int:
					designatedTimestamp = q.designatedTimestampUnits.From(int64(val))
				case int32:
					designatedTimestamp = q.designatedTimestampUnits.From(int64(val))
				case int64:
					designatedTimestamp = q.designatedTimestampUnits.From(val)
				default:
					// todo: exit here or no?
					q.log.Errorf("Unsupported type %T for designated timestamp field %v", v, k)
				}

				continue
			}

			switch val := v.(type) {
			case string:
				if _, found := q.timestampStringFields[k]; found {
					t, err := time.Parse(q.timestampStringFormat, val)
					if err != nil {
						return fmt.Errorf("QuestDB error: could not parse timestamp %v in field %v: %w", val, k, err)
					}
					sender.TimestampColumn(k, t)
					continue
				}

				sender.StringColumn(k, val)
			case int:
			case int32:
			case int64:
				sender.Int64Column(k, val)
			case float32:
				sender.Float64Column(k, float64(val))
			case float64:
				sender.Float64Column(k, val)
			case bool:
				sender.BoolColumn(k, val)
			case json.Number:
				f, err := val.Float64()
				if err != nil {
					return err
				}
				sender.Float64Column(k, f)
			case time.Time:
				sender.TimestampColumn(k, val)
			case map[string]any: // Assuming nested JSON object handling if needed
				// todo: flatten nested JSON object
				q.log.Errorf("Unsupported type %T for field %v", v, k)
			default:
				msg := fmt.Sprintf("Unsupported type %T for field %v", v, k)
				if q.skipUnsupportedTypes {
					q.log.Warn(msg)
				} else {
					q.log.Errorf(msg)
				}
			}
		}

		if !designatedTimestamp.IsZero() {
			if err := sender.At(ctx, designatedTimestamp); err != nil {
				return err
			}
		} else {
			if err := sender.AtNow(ctx); err != nil {
				return err
			}
		}
	}
	return sender.Flush(ctx)
}

func walkForFields(msg *service.Message, fields map[string]any) error {
	jVal, err := msg.AsStructured()
	if err != nil {
		return err
	}
	jObj, ok := jVal.(map[string]any)
	if !ok {
		return fmt.Errorf("expected JSON object, found '%T'", jVal)
	}
	for k, v := range jObj {
		fields[k] = v
	}
	return nil
}

func (q *questdbWriter) Close(ctx context.Context) error {
	return q.pool.Close(ctx)
}

func init() {
	if err := service.RegisterBatchOutput(
		"questdb",
		questdbOutputConfig(),
		fromConf,
	); err != nil {
		panic(err)
	}
}
