package questdb

import (
	"math"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
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
	/*
			res := service.MockResources()

			testCases := []struct {
				name          string
				conf          string
				assertionFunc func(*questdbWriter) bool
			}{
				{
					name: "basic",
					conf: `
		table: test
		client_conf_string: "http::addr=localhost:9000"
		`,
					assertionFunc: func(w *questdbWriter) bool {
						w.table = "test"
						s, err := w.pool.Acquire()
					} questdbWriter{
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

					assert.True(t, tc.assertionFunc(writer))

				})
			}
	*/
}

func TestValidationErrorsFromConf(t *testing.T) {
	testCases := []struct {
		name                string
		conf                string
		expectedErrContains string
	}{
		{
			name:                "no client_conf_string",
			conf:                "table: test",
			expectedErrContains: "field 'client_conf_string' is required",
		},
		{
			name:                "no table",
			conf:                `client_conf_string: "http::addr=localhost:9000"`,
			expectedErrContains: "field 'table' is required",
		},
		{
			name: "invalid timestamp unit",
			conf: `
client_conf_string: "http:addr=localhost:9000"
table: test
designatedTimestampUnits: hello`,
			expectedErrContains: "is not a valid timestamp unit",
		},
	}

	for _, tc := range testCases {
		configSpec := questdbOutputConfig()

		t.Run(tc.name, func(t *testing.T) {
			cfg, err := configSpec.ParseYAML(tc.conf, nil)
			if err != nil {
				assert.ErrorContains(t, err, tc.expectedErrContains)
				return
			}

			_, _, _, err = fromConf(cfg, service.MockResources())
			assert.ErrorContains(t, err, tc.expectedErrContains)

		})
	}
}
