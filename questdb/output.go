package questdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
			service.NewStringField("token").
				Description("Bearer token for HTTP auth (takes precedence over basic auth username & password)").
				Optional().
				Secret(),
			service.NewBoolField("tlsEnabled").
				Description("Use TLS to secure the connection to the server").
				Optional(),
			service.NewStringField("tlsVerify").
				Description("Whether to verify the server's certificate. This should only be used for testing as a last resort "+
					"and never used in production as it makes the connection vulnerable to man-in-the-middle attacks. Options are 'on' or 'unsafe_off'.").
				Optional().
				Default("on").
				LintRule(`root = if ["on","unsafe_off"].contains(this != true { ["valid options are \"on\" or \"unsafe_off\"" ] }`),
			service.NewDurationField("retryTimeout").
				Description("The time to continue retrying after a failed HTTP request. The interval between retries is an exponential "+
					"backoff starting at 10ms and doubling after each failed attempt up to a maximum of 1 second.").
				Optional().
				Advanced(),
			service.NewDurationField("requestTimeout").
				Description("The time to wait for a response from the server. This is in addition to the calculation derived from the requestMinThroughput parameter.").
				Optional().
				Advanced(),
			service.NewIntField("requestMinThroughput").
				Description("Minimum expected throughput in bytes per second for HTTP requests. If the throughput is lower than this value, "+
					"the connection will time out. This is used to calculate an additional timeout on top of requestTimeout. This is useful for large requests. "+
					"You can set this value to 0 to disable this logic.").
				Optional().
				Advanced(),
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
			service.NewBoolField("errorOnEmptyMessages").
				Description("Mark a message as errored if it is empty after field validation").
				Optional().
				Default(false),
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
	errorOnEmptyMessages     bool
}

func fromConf(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {

	opts := []qdb.LineSenderOption{
		qdb.WithHttp(),
		qdb.WithAutoFlushDisabled(),
	}

	if conf.Contains("tlsEnabled") {
		var tls bool
		if tls, err = conf.FieldBool("tlsEnabled"); err != nil {
			return
		}
		if tls {
			opts = append(opts, qdb.WithTls())
		}
	}

	var tlsVerify string
	if tlsVerify, err = conf.FieldString("tlsVerify"); err != nil {
		return
	}
	switch tlsVerify {
	case "on":
		break
	case "unsafe_off":
		opts = append(opts, qdb.WithTlsInsecureSkipVerify())
	default:
		err = fmt.Errorf("invalid tlsVerify setting: %s", tlsVerify)
		return
	}

	var addr string
	if addr, err = conf.FieldString("address"); err != nil {
		return
	}
	opts = append(opts, qdb.WithAddress(addr))

	if token, _ := conf.FieldString("token"); token != "" {
		opts = append(opts, qdb.WithBearerToken(token))
	} else {
		username, _ := conf.FieldString("username")
		password, _ := conf.FieldString("password")
		if username != "" && password != "" {
			opts = append(opts, qdb.WithBasicAuth(username, password))

		}
	}

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

	w.pool, err = qdb.PoolFromOptions(opts...)
	if err != nil {
		return
	}

	qdb.WithMaxSenders(mif)(w.pool)

	if conf.Contains("retryTimeout") {
		var retryTimeout time.Duration
		if retryTimeout, err = conf.FieldDuration("retryTimeout"); err != nil {
			return
		}
		qdb.WithRetryTimeout(retryTimeout)
	}

	if conf.Contains("requestTimeout") {
		var requestTimeout time.Duration
		if requestTimeout, err = conf.FieldDuration("requestTimeout"); err != nil {
			return
		}
		qdb.WithRequestTimeout(requestTimeout)
	}

	if conf.Contains("requestMinThroughput") {
		var requestMinThroughput int
		if requestMinThroughput, err = conf.FieldInt("requestMinThroughput"); err != nil {
			return
		}
		qdb.WithMinThroughput(requestMinThroughput)
	}

	if w.table, err = conf.FieldString("table"); err != nil {
		return
	}

	var symbols []string
	if conf.Contains("symbols") {
		if symbols, err = conf.FieldStringList("symbols"); err != nil {
			return
		}
		for _, s := range symbols {
			w.symbols[s] = true
		}
	}

	var timestampFields []string
	if conf.Contains("timestampFields") {
		if timestampFields, err = conf.FieldStringList("timestampFields"); err != nil {
			return
		}
		for _, f := range timestampFields {
			w.timestampFields[f] = true
		}
	}

	if conf.Contains("designatedTimestampField") {
		if w.designatedTimestampField, err = conf.FieldString("designatedTimestampField"); err != nil {
			return
		}
	}

	var timestampUnits string
	if conf.Contains("timestampUnits") {
		if timestampUnits, err = conf.FieldString("timestampUnits"); err != nil {
			return
		}

		// perform validation on timestamp units here in case the user doesn't lint the config
		w.timestampUnits = timestampUnit(timestampUnits)
		if !w.timestampUnits.IsValid() {
			err = fmt.Errorf("%v is not a valid timestamp unit", timestampUnits)
			return
		}
	}

	if conf.Contains("timestampStringFormat") {
		if w.timestampStringFormat, err = conf.FieldString("timestampStringFormat"); err != nil {
			return
		}

	}

	if w.errorOnEmptyMessages, err = conf.FieldBool("errorOnEmptyMessages"); err != nil {
		return
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
			q.log.Errorf("could not parse timestamp field %v", err)
		}
		return t, err
	case json.Number:
		intVal, err := val.Int64()
		if err != nil {
			q.log.Errorf("numerical timestamps must be int64: %v", err)
		}
		return q.timestampUnits.From(intVal), err
	default:
		err := fmt.Errorf("unsupported type %T for designated timestamp: %v", v, v)
		q.log.Error(err.Error())
		return time.Time{}, err
	}
}

func (q *questdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) (err error) {
	var (
		sender qdb.LineSender
	)

	sender, err = q.pool.Sender(ctx)
	if err != nil {
		return err
	}

	defer func() {
		// This will flush the sender, no need to call sender.Flush at the end of the method
		releaseErr := sender.Close(ctx)
		if releaseErr != nil {
			if err != nil {
				err = fmt.Errorf("%v %w", err, releaseErr)
			} else {
				err = releaseErr
			}
		}
	}()

	return batch.WalkWithBatchedErrors(func(i int, m *service.Message) (err error) {
		// QuestDB's LineSender constructs ILP messages using a buffer, so message
		// components must be written in the correct order, otherwise the sender will
		// return an error. This order is:
		// 1. Table Name
		// 2. Symbols (key/value pairs)
		// 3. Columns (key/value pairs)
		// 4. Timestamp [optional]
		//
		// Before writing any column, we call ensureTable(), which is guaranteed to run once
		// and writes the table name before any other message data. Since it is also illegal
		// to send an empty message (no symbols or columns), we also track if we've written
		// anything by checking hasTable, which is set to true inside ensureTable().
		var (
			hasTable    bool
			ensureTable = sync.OnceFunc(func() {
				sender.Table(q.table)
				hasTable = true
			})
		)

		defer func() {
			if err != nil {
				m.SetError(err)
			}
		}()

		q.log.Tracef("Writing message %v", i)

		// Fields must be written in a particular order by type, so
		// we use a mutable obj to delete fields written at an earlier stage
		// of the message construction process.
		jVal, err := m.AsStructuredMut()
		if err != nil {
			return fmt.Errorf("unable to parse JSON: %v", err)
		}
		jObj, ok := jVal.(map[string]any)
		if !ok {
			return fmt.Errorf("expected JSON object, found '%T'", jVal)
		}

		// Stage 1: Handle all symbols, which must be written to the buffer first
		for s := range q.symbols {
			val, found := jObj[s]
			if found {
				ensureTable()
				sender.Symbol(s, fmt.Sprintf("%v", val))

				// Delete the symbol so we don't iterate over it in the next step
				delete(jObj, s)
			}
		}

		// Stage 2: Handle columns
		for k, v := range jObj {

			// Skip designated timestamp field (will process this in the 3rd stage)
			if q.designatedTimestampField != "" && q.designatedTimestampField == k {
				continue
			}

			// Check if the field is a timestamp and process accordingly
			if _, isTimestampField := q.timestampFields[k]; isTimestampField {
				timestamp, err := q.parseTimestamp(v)
				if err == nil {
					ensureTable()
					sender.TimestampColumn(k, timestamp)
				} else {
					q.log.Errorf("%v", err)
				}
				continue
			}

			// For all non-timestamp fields, process values by JSON types since we are working
			// with structured messages
			switch val := v.(type) {
			case string:
				ensureTable()
				sender.StringColumn(k, val)
			case bool:
				ensureTable()
				sender.BoolColumn(k, val)
			case json.Number:
				// For json numbers, first attempt to parse as int, then fall back to float
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
						q.log.Errorf("could not parse %v into a number: %v", val, err)
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

		// Stage 3: Handle designated timestamp and finalize the buffered message
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
			if q.errorOnEmptyMessages {
				return errors.New("empty message, skipping send to QuestDB")
			}
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
