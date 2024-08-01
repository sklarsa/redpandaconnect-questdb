package questdb

import (
	"math"
	"testing"
	"time"

	"github.com/questdb/go-questdb-client/v3"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampConversions(t *testing.T) {

	testCases := []struct {
		name         string
		value        int64
		unit         timestampUnit
		expectedTime time.Time
	}{
		{
			name:         "autoSecondsMin",
			value:        0,
			unit:         auto,
			expectedTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "autoSecondsMax",
			value:        9999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 0, time.UTC),
		},
		{
			name:         "autoMillisMin",
			value:        10000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoMillisMax",
			value:        9999999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 999000000, time.UTC),
		},
		{
			name:         "autoMicrosMin",
			value:        10000000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoMicrosMax",
			value:        9999999999999999,
			unit:         auto,
			expectedTime: time.Date(2286, 11, 20, 17, 46, 39, 999999000, time.UTC),
		},
		{
			name:         "autoNanosMin",
			value:        10000000000000000,
			unit:         auto,
			expectedTime: time.Date(1970, 4, 26, 17, 46, 40, 0, time.UTC),
		},
		{
			name:         "autoNanosMax",
			value:        math.MaxInt64,
			unit:         auto,
			expectedTime: time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedTime, tc.unit.From(tc.value))
		})
	}
}

func TestFromConf(t *testing.T) {
	testCases := []struct {
		name          string
		conf          string
		expected      questdbWriter
		assertionFunc func()
	}{
		{
			name: "basic",
			conf: `
table: test
client_conf_string: "http::addr=localhost:9000"
`,
			expected: questdbWriter{
				pool:  questdb.PoolFromConf("http::addr=localhost:9000", questdb.WithMaxSenders(64)),
				table: "test",
			},
		},
	}

	configSpec := questdbOutputConfig()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := configSpec.ParseYAML(tc.conf, nil)
			require.NoError(t, err)

			out, _, _, err := fromConf(parsed, service.MockResources())
			require.NoError(t, err)

			writer, ok := out.(*questdbWriter)
			require.True(t, ok)

			assert.Equal(t, &tc.expected, writer)

		})
	}
}

func TestValidationErrorsFromConf(t *testing.T) {
}
