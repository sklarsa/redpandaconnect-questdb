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

func clientFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField("client_conf_string").
			Description("QuestDB client configuration string").
			Secret().
			Example("http::addr=localhost:9000;"),
		service.NewStringField("table").
			Description("Destination table").
			Example("trades"),
		service.NewBoolField("skipUnsupportedTypes").
			Description("Skips unsupported types").
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
			Default(time.StampMicro + "Z0700").
			Optional(),
		service.NewStringListField("symbols").
			Description("Columns that should be the SYMBOL type (string values default to STRING)").
			Optional(),
	}
}

func questdbOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Pushes messages a QuestDB table").
		Description(`The field `+"`key`"+` supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing you to create a unique key for each message.`+service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(loFieldBatching),
		)
}

type questdbWriter struct {
	log *service.Logger

	key *service.InterpolatedString

	senderCtor func(ctx context.Context) (*qdb.LineSenderPool, error)

	pool *qdb.LineSenderPool

	skipUnsupportedTypes bool
	symbols              map[string]bool
	table                string
	timestampField       string
}

func (q *questdbWriter) Connect(ctx context.Context) error {
	pool, err := q.senderCtor(ctx)
	if err != nil {
		return err
	}
	q.pool = pool
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
			q.log.Errorf("QuestDB error: %v\n", err)
			return err
		}

		for k, v := range fields {

			if _, found := q.symbols[k]; found {
				sender.Symbol(k, fmt.Sprintf("%v", v))
				continue
			}

			if k == q.timestampField {
				// todo: parse designated timestamp based on type
				panic("not implemented")
			}

			switch val := v.(type) {
			case string:
				sender.StringColumn(k, val)
			case int:
				sender.Int64Column(k, int64(val))
			case int32:
				sender.Int64Column(k, int64(val))
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
	err := service.RegisterBatchOutput(
		"questdb", questdbOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(loFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}

			table, err := conf.FieldString("table")
			if err != nil {
				return
			}

			symbols := map[string]bool{}
			symbolsList, err := conf.FieldStringList("symbols")
			if err != nil && strings.Contains(err.Error(), "expected field") {
				return
			}
			for _, s := range symbolsList {
				symbols[s] = true
			}

			timestampField, err := conf.FieldString("timestampField")
			if err != nil && strings.Contains(err.Error(), "expected field") {
				return
			}

			skipUnsupportedTypes, err := conf.FieldBool("skipUnsupportedTypes")
			if err != nil && strings.Contains(err.Error(), "expected field") {
				return
			}

			out = &questdbWriter{
				log: mgr.Logger(),
				senderCtor: func(ctx context.Context) (*qdb.LineSenderPool, error) {
					clientConfStr, err := conf.FieldString("client_conf_string")
					return qdb.PoolFromConf(clientConfStr), err
				},
				skipUnsupportedTypes: skipUnsupportedTypes,
				symbols:              symbols,
				table:                table,
				timestampField:       timestampField,
			}

			return
		})
	if err != nil {
		panic(err)
	}
}
