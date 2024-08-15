package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	qdb "github.com/questdb/go-questdb-client/v3"
)

func TestIntegrationQuestDBBasic(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	pool := setupDockertestPool(t)
	resource := setupQuestDB(t, ctx, pool)

	t.Parallel()

	template := `
output:
  questdb:
    address: "localhost:$PORT"
    table: $ID
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		pgConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@localhost:%v", resource.GetPort("8812/tcp")))
		require.NoError(t, err)
		defer pgConn.Close(ctx)

		result := pgConn.ExecParams(ctx, fmt.Sprintf("SELECT content, id FROM '%v' WHERE id=%v", testID, messageID), nil, nil, nil, nil)

		result.NextRow()
		id, err := strconv.Atoi(string(result.Values()[1]))
		assert.NoError(t, err)
		data := map[string]any{
			"content": string(result.Values()[0]),
			"id":      id,
		}

		assert.False(t, result.NextRow())

		outputBytes, err := json.Marshal(data)
		require.NoError(t, err)
		return string(outputBytes), nil, nil
	}

	suite := integration.StreamTests(
		integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("9000/tcp")),
	)
}

func TestIntegrationQuestDBConfigOptions(t *testing.T) {
	testCases := []struct {
		name      string
		qdbConfig string
		input     []string
		validate  func(context.Context, *pgconn.PgConn)
	}{}

	pool := setupDockertestPool(t)
	ctx := context.Background()

	t.Parallel()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			qdb := setupQuestDB(t, ctx, pool)
			// todo: pass qdb port to qdbConfig
			benthos := setupBenthos(t, pool, tc.qdbConfig)

			for _, line := range tc.input {
				resp, err := http.Post(
					fmt.Sprintf("http://localhost:%s", benthos.GetPort("4195/tcp")),
					"application/json",
					strings.NewReader(line),
				)

				require.NoError(t, err)
				_, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				resp.Body.Close()

			}

			pgConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@localhost:%v", qdb.GetPort("8812/tcp")))
			require.NoError(t, err)
			defer pgConn.Close(ctx)

			tc.validate(ctx, pgConn)

		})
	}

}

func setupDockertestPool(t *testing.T) *dockertest.Pool {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute * 3
	return pool

}

func setupQuestDB(t *testing.T, ctx context.Context, pool *dockertest.Pool) *dockertest.Resource {

	resource, err := pool.Run("questdb/questdb", "8.0.0", []string{
		"JAVA_OPTS=-Xms512m -Xmx512m",
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

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

	return resource
}

func setupBenthos(t *testing.T, pool *dockertest.Pool, qdbConf string) *dockertest.Resource {
	tmp := t.TempDir()

	// todo: construct basic config (http input) and inject output config with qdbconf

	require.NoError(t, os.WriteFile(path.Join(tmp, "config.yaml"), []byte(conf), 0444))

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "jeffail/benthos",
		Tag:        "4.27",
		Mounts: []string{
			path.Join(tmp, "config.yaml", "/benthos.yaml"),
		},
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	// todo: wait until container http server is listening...

	_ = resource.Expire(900)

	return resource
}
