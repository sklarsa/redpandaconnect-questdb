package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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

func (t timestampUnit) IsValid() bool {
	return t == nanos ||
		t == micros ||
		t == millis ||
		t == seconds ||
		t == auto
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
			service.NewStringField("address").
				Description("Address of the QuestDB server's HTTP port (excluding protocol)").
				Example("localhost:9000"),
			service.NewStringField("username").
				Description("Username for HTTP basic auth").
				Optional().
				Secret(),
			service.NewStringField("password").
				Description("Password for HTTP basic auth").
				Optional().
				Secret(),
			service.NewBoolField("tls_enabled").
				Description("Use TLS to secure the connection to the server").
				Optional(),
			service.NewStringField("tls_verify").
				Description("Whether to verify the server's certificate. This should only be used for testing as a last resort and never used in production as it makes the connection vulnerable to man-in-the-middle attacks. Options are 'on' or 'unsafe_off'.").
				Optional().
				LintRule(`root = if ["on","unsafe_off"].contains(this != true { ["valid options are \"on\" or \"unsafe_off\"" ] }`),
			service.NewStringField("token").
				Description("Bearer token for HTTP auth (used in lieu of basic auth)").
				Secret(),
			service.NewStringField("table").
				Description("Destination table").
				Example("trades"),
			service.NewStringField("designatedTimestampField").
				Description("Name of the designated timestamp field").
				Optional(),
			service.NewStringField("timestampUnits").
				Description("Designated timestamp field units").
				Default("auto").
				LintRule(`root = if ["nanos","micros","millis","seconds","auto"].contains(this) != true { [ "valid options are \"nanos\", \"micros\", \"millis\", \"seconds\", \"auto\"" ] }`).
				Optional(),
			service.NewStringListField("timestampFields").
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

	symbols                  map[string]bool
	table                    string
	designatedTimestampField string
	timestampUnits           timestampUnit
	timestampStringFormat    string
	timestampFields          map[string]bool
}

func fromConf(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
	var (
		confString = &strings.Builder{}
	)

	confString.WriteString("http")
	if tls, _ := conf.FieldBool("tls_enabled"); tls {
		confString.WriteRune('s')
	}

	confString.WriteString("::addr")

	var addr string
	if addr, err = conf.FieldString("address"); err != nil {
		return
	}
	confString.WriteString(addr)

	if token, _ := conf.FieldString("token"); token != "" {
		confString.WriteString(",token=")
		confString.WriteString(token)
	} else {
		username, _ := conf.FieldString("username")
		password, _ := conf.FieldString("password")
		if username != "" && password != "" {
			confString.WriteString(",username=")
			confString.WriteString(username)
			confString.WriteString(",password=")
			confString.WriteString(password)

		}
	}

	confString.WriteString(",auto_flush=off")

	w := &questdbWriter{
		log:             mgr.Logger(),
		symbols:         map[string]bool{},
		timestampFields: map[string]bool{},
	}
	out = w

	if batchPol, err = conf.FieldBatchPolicy(loFieldBatching); err != nil {
		return
	}

	if mif, err = conf.FieldMaxInFlight(); err != nil {
		return
	}

	w.pool, err = qdb.PoolFromConf(confString.String(), qdb.WithMaxSenders(mif))
	if err != nil {
		return
	}

	if w.table, err = conf.FieldString("table"); err != nil {
		return
	}

	var symbols []string
	symbols, err = conf.FieldStringList("symbols")
	if err != nil {
		return
	}
	for _, s := range symbols {
		w.symbols[s] = true
	}

	var timestampFields []string
	if timestampFields, err = conf.FieldStringList("timestampFields"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}

	}
	for _, f := range timestampFields {
		w.timestampFields[f] = true
	}

	if w.designatedTimestampField, err = conf.FieldString("designatedTimestampField"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}

	var timestampUnits string
	if timestampUnits, err = conf.FieldString("timestampUnits"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}
	// perform validation on timestamp units here in case the user doesn't lint the config
	w.timestampUnits = timestampUnit(timestampUnits)
	if !w.timestampUnits.IsValid() {
		err = fmt.Errorf("%v is not a valid timestamp unit", timestampUnits)
		return
	}

	if w.timestampStringFormat, err = conf.FieldString("timestampStringFormat"); err != nil {
		if !strings.Contains(err.Error(), "was not found in the config") {
			return
		}
	}

	return
}

func (q *questdbWriter) Connect(ctx context.Context) error {
	// No connections are required to initialize a LineSenderPool,
	// so nothing to do here. Each LineSender has its own http client
	// that will use the network only when flushing messages to the server.
	return nil
}

func (q *questdbWriter) parseTimestamp(v any) (time.Time, error) {
	switch val := v.(type) {
	case string:
		t, err := time.Parse(q.timestampStringFormat, val)
		if err != nil {
			q.log.Errorf("QuestDB error: could not parse timestamp field %v", err)
		}
		return t, err
	case json.Number:
		intVal, err := val.Int64()
		if err != nil {
			q.log.Errorf("QuestDB error: numerical timestamps must be int64: %v", err)
		}
		return q.timestampUnits.From(intVal), err
	default:
		err := fmt.Errorf("QuestDB error: unsupported type %T for designated timestamp: %v", v, v)
		q.log.Error(err.Error())
		return time.Time{}, err
	}
}

func (q *questdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	var (
		err    error
		sender qdb.LineSender
	)

	sender, err = q.pool.Acquire(ctx)
	if err != nil {
		q.log.Errorf("QuestDB error: %v", err)
		return err
	}

	panic("figure out which errors to return for the desired behavior of retrying the entire batch on a flush fail, or drop malformed messages? idk..")

	defer func() {
		// This will flush the sender, no need to call sender.Flush at the end of the method
		tmpErr := q.pool.Release(ctx, sender)
		if tmpErr != nil {
			q.log.Errorf("QuestDB error: %v", tmpErr)
		}
	}()

	return batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		var (
			hasTable    bool
			ensureTable = sync.OnceFunc(func() {
				sender.Table(q.table)
				hasTable = true
			})
		)

		q.log.Tracef("Writing message %v", i)

		jVal, err := m.AsStructuredMut()
		if err != nil {
			return fmt.Errorf("unable to parse json: %v", err)
		}
		jObj, ok := jVal.(map[string]any)
		if !ok {
			return fmt.Errorf("expected JSON object, found '%T'", jVal)
		}

		// First handle symbols
		for s := range q.symbols {
			val, found := jObj[s]
			if found {
				ensureTable()
				sender.Symbol(s, fmt.Sprintf("%v", val))
				delete(jObj, s)
			}
		}

		// Then handle columns
		for k, v := range jObj {

			// Skip designated timestamp field
			if q.designatedTimestampField != "" && q.designatedTimestampField == k {
				continue
			}

			// Then check if the field is a timestamp
			if _, isTimestampField := q.timestampFields[k]; isTimestampField {
				timestamp, err := q.parseTimestamp(v)
				if err == nil {
					ensureTable()
					sender.TimestampColumn(k, timestamp)
				}
				continue
			}

			// For all non-timestamp fields, process values by JSON types since we are working with structured messages
			switch val := v.(type) {
			case string:
				ensureTable()
				sender.StringColumn(k, val)
			case bool:
				ensureTable()
				sender.BoolColumn(k, val)
			case json.Number:
				// For json numbers, first attempt to parse as int, then fallback to float
				intVal, err := val.Int64()
				if err == nil {
					ensureTable()
					sender.Int64Column(k, intVal)
				} else {
					floatVal, err := val.Float64()
					if err == nil {
						ensureTable()
						sender.Float64Column(k, floatVal)
					} else {
						q.log.Errorf("QuestDB error: could not parse %v into a number: %v", val, err)
					}
				}
			case float64:
				// float64 is only needed if BENTHOS_USE_NUMBER=false
				ensureTable()
				sender.Float64Column(k, float64(val))
			default:
				q.log.Errorf("unsupported type %T for field %v", v, k)
			}
		}

		// Finally handle designated timestamp
		var designatedTimestamp time.Time
		if q.designatedTimestampField != "" {
			val, found := jObj[q.designatedTimestampField]
			if found {
				designatedTimestamp, err = q.parseTimestamp(val)
				if err != nil {
					q.log.Errorf("unable to parse designated timestamp: %v", val)
				}
			}
		}

		if !hasTable {
			q.log.Warn("empty message, skipping send to QuestDB")
			return nil
		}

		if !designatedTimestamp.IsZero() {
			err = sender.At(ctx, designatedTimestamp)
		} else {
			err = sender.AtNow(ctx)
		}

		return err
	})

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
