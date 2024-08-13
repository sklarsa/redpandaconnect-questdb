package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import full suite of FOSS connect plugins
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	// Bring in the internal plugin definitions.
	_ "github.com/sklarsa/redpandaconnect-questdb/questdb"
)

func main() {
	service.RunCLI(context.Background())
}
