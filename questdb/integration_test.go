package questdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	qdb "github.com/questdb/go-questdb-client/v3"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationQuestDB(t *testing.T) {
	ctx := context.Background()

	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("questdb/questdb", "8.0.0", []string{
		"JAVA_OPTS=-Xms512m -Xmx512m",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	// Wait for QuestDB to boot up
	if err = pool.Retry(func() error {
		clientConfStr := fmt.Sprintf("http::addr=localhost:%v", resource.GetPort("9000/tcp"))
		sender, err := qdb.LineSenderFromConf(ctx, clientConfStr)
		if err != nil {
			return err
		}
		defer sender.Close(ctx)
		err = sender.Table("ping").Int64Column("test", 42).AtNow(ctx)
		if err != nil {
			return err
		}
		return sender.Flush(ctx)
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	for _, tc := range []struct {
		name         string
		qdbConfig    string
		payload      []string
		query        string
		validateFunc func(t *testing.T, pc *pgconn.ResultReader)
	}{
		{
			"basicIngest",
			"",
			[]string{`{"hello": "world", "test": 1}`},
			"SELECT hello, test FROM 'basicIngest'",
			func(t *testing.T, r *pgconn.ResultReader) {
				assert.True(t, r.NextRow())
				assert.Equal(t, 2, len(r.Values()))
				assert.Equal(t, "world", string(r.Values()[0]))
				assert.Equal(t, "1", string(r.Values()[1]))
			},
		},
		{
			"withDesignatedTimestamp",
			"designatedTimestampField: timestamp",
			[]string{`{"hello": "world", "timestamp": 10000000000 }`},
			"SELECT timestamp FROM 'withDesignatedTimestamp'",
			func(t *testing.T, r *pgconn.ResultReader) {
				assert.True(t, r.NextRow())
				assert.Equal(t, 1, len(r.Values()))
				assert.Equal(t, "1970-04-26 17:46:40.000000", string(r.Values()[0]))
			},
		},
		{
			"withDesignatedTimestampUnit",
			`designatedTimestampField: timestamp
designatedTimestampUnit: millis`,
			[]string{`{"hello": "world", "timestamp": 1 }`},
			"SELECT timestamp FROM 'withDesignatedTimestampUnit'",
			func(t *testing.T, r *pgconn.ResultReader) {
				assert.True(t, r.NextRow())
				assert.Equal(t, 1, len(r.Values()))
				assert.Equal(t, "1970-01-01 00:00:00.001000", string(r.Values()[0]))
			},
		},
		// todo: "withTimestampStringFields"
		// todo: "withTimestampStringFormats"
		// todo: "withDoubles" --> need to implement

	} {
		t.Run(tc.name, func(t *testing.T) {
			//t.Parallel()

			conf := fmt.Sprintf(
				"address: \"%s\"\ntable: \"%s\"\n%s",
				fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp")),
				tc.name,
				tc.qdbConfig,
			)

			spec := questdbOutputConfig()
			cfg, err := spec.ParseYAML(conf, service.NewEmptyEnvironment())
			require.NoError(t, err)
			out, _, _, err := fromConf(cfg, service.MockResources())
			require.NoError(t, err)

			require.NoError(t, out.Connect(ctx))

			qdbWriter, ok := out.(*questdbWriter)
			require.True(t, ok)

			batch := service.MessageBatch{}
			for _, line := range tc.payload {
				batch = append(batch, service.NewMessage([]byte(line)))
			}
			require.NoError(t, qdbWriter.WriteBatch(ctx, batch))

			pgConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@localhost:%v", resource.GetPort("8812/tcp")))
			require.NoError(t, err)
			defer pgConn.Close(ctx)

			result := pgConn.ExecParams(ctx, tc.query, nil, nil, nil, nil)
			tc.validateFunc(t, result)
			_, err = result.Close()
			assert.NoError(t, err)

		})
	}
}
