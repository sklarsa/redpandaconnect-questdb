package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	if err = pool.Retry(func() error {
		clientConfStr := fmt.Sprintf("http::addr=localhost:%v;", resource.GetPort("9000/tcp"))
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

	template := `
output:
  questdb:
    client_conf_string: "http::addr=localhost:$PORT;"
    table: $ID
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%v/exec", resource.GetPort("9000/tcp")), nil)

		q := req.URL.Query()
		q.Add("query", fmt.Sprintf("SELECT * from '%v' where id = %v", testID, messageID))
		req.URL.RawQuery = q.Encode()

		if err != nil {
			return "", nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", nil, err
		}
		defer resp.Body.Close()

		var data map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&data)
		if err != nil {
			return "", nil, err
		}
		count, ok := data["count"].(float64)
		if !ok {
			return "", nil, fmt.Errorf("bad count type, expected float64, got %T", data["count"])
		}

		if int(count) != 1 {
			return "", nil, fmt.Errorf("bad row count, expected 1, got %v", data["count"])
		}

		columns := data["columns"]
		row := data["dataset"].([]interface{})[0].([]interface{})
		if len(columns.([]interface{})) != len(row) {
			return "", nil, fmt.Errorf("column count mismatch, expected %v, got %v", len(columns.([]interface{})), len(row))
		}

		output := map[string]interface{}{}
		for i, col := range columns.([]interface{}) {
			col0 := col.(map[string]interface{})
			colName := col0["name"].(string)
			// Skip timestamp column, it's synthetic, added by QuestDB
			if colName != "timestamp" {
				output[colName] = row[i]
			}
		}
		outputBytes, err := json.Marshal(output)
		if err != nil {
			return "", nil, err
		}
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
