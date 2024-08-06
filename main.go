package main

import (
	"context"
	"log"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import full suite of FOSS connect plugins
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	// Bring in the internal plugin definitions.
	_ "github.com/sklarsa/redpandaconnect-questdb/questdb"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	service.RunCLI(context.Background())
}
