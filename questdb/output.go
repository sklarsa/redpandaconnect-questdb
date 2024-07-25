package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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
		service.NewStringField("timestampField").
			Description("Name of the designated timestamp field"),
		service.NewStringListField("symbols").
			Description("Columns that should be the SYMBOL type (string values default to STRING)"),
	}
}

func questdbOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary(`Pushes messages a QuestDB table`).
		Description(`The field `+"`key`"+` supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing you to create a unique key for each message.`+service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewOutputMaxInFlightField().Default(75000),
			service.NewBatchPolicyField(loFieldBatching),
		)
}

type questdbWriter struct {
	log *service.Logger

	key *service.InterpolatedString

	senderCtor func(ctx context.Context) (qdb.LineSender, error)
	senderMut  sync.RWMutex
	sender     qdb.LineSender
	table      string
}

func (q *questdbWriter) Connect(ctx context.Context) error {
	q.senderMut.Lock()
	defer q.senderMut.Unlock()
	sender, err := q.senderCtor(ctx)
	if err != nil {
		return err
	}
	q.sender = sender
	return nil
}

func (q *questdbWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	q.senderMut.Lock()
	defer q.senderMut.Unlock()
	sender := q.sender
	if sender == nil {
		return service.ErrNotConnected
	}
	batchSize := len(batch)
	for i := 0; i < batchSize; i++ {
		q.log.Debugf("Writing message %v", i)

		sender.Table(q.table)
		msg := batch[i]

		fields := map[string]any{}

		if err := walkForFields(msg, fields); err != nil {
			err = fmt.Errorf("failed to walk JSON object: %v", err)
			q.log.Errorf("QuestDB error: %v\n", err)
			return err
		}

		for k, v := range fields {
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
				q.log.Errorf("Unsupported type %T for field %v", v, k)
			}
		}

		// todo: handle designated timestamps
		err := sender.AtNow(ctx)
		if err != nil {
			return err
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
	q.senderMut.Lock()
	defer q.senderMut.Unlock()
	sender := q.sender
	if sender != nil {
		return sender.Close(ctx)
	}
	return nil
}

func newQuestdbWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *questdbWriter, err error) {
	table, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}
	r = &questdbWriter{
		log: mgr.Logger(),
		senderCtor: func(ctx context.Context) (qdb.LineSender, error) {
			clientConfStr, err := conf.FieldString("client_conf_string")
			if err != nil {
				return nil, err
			}
			sender, err := qdb.LineSenderFromConf(ctx, clientConfStr)
			return sender, err
		},
		table: table,
	}

	return r, nil
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
			out, err = newQuestdbWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}
