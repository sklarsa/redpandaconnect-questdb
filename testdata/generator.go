package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	rand "math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	_ "net/http/pprof"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	const genThreads = 1
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	http.DefaultTransport.(*http.Transport).MaxIdleConns = 100

	g := &generator{
		bufSize: 10000,
		fields: map[string]field{
			"test":        int64Field{},
			"testString":  stringField{length: 24},
			"testFloat32": float32Field{},
			"testFloat64": float64Field{},
			"testint64":   int64Field{},
		},
		once: &sync.Once{},

		genThreads: genThreads,

		benthosEndpoint: "http://localhost:4195/post",
	}

	g.start()

	for {
		time.Sleep(time.Second * 5)
	}
}

type generator struct {
	bufSize int

	fields map[string]field
	once   *sync.Once

	buf chan []byte

	genThreads int

	benthosEndpoint string
}

func (g *generator) start() {
	g.once.Do(func() {
		g.buf = make(chan []byte, g.bufSize)
		for range g.genThreads {
			go func() {
				for {
					msgData := map[string]any{}
					for k, v := range g.fields {
						msgData[k] = v.value()
					}

					msg, err := json.Marshal(msgData)
					if err != nil {
						panic(err)
					}

					g.buf <- msg
				}
			}()
		}

		go func() {
			const batchSize = 100000

			conn, err := net.Dial("tcp", "0.0.0.0:6000")
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			for {
				start := time.Now()
				for i := 0; i < batchSize; i++ {
					_, err := conn.Write(<-g.buf)
					if err != nil {
						panic(err)
					}
					_, err = conn.Write([]byte("\n"))
					if err != nil {
						panic(err)
					}
				}
				rate := batchSize * (1000 / float64(time.Since(start).Milliseconds()))
				fmt.Printf("rate: %f rows/s\n", rate)
			}
		}()
	})
}

type field interface {
	value() any
}

type int64Field struct {
}

func (f int64Field) value() any {
	return rand.Int64N(math.MaxInt64)
}

type float32Field struct {
}

func (f float32Field) value() any {
	return rand.Float32()
}

type float64Field struct {
}

func (f float64Field) value() any {
	return rand.Float64()
}

type stringField struct {
	length int
}

func (f stringField) value() any {
	return "just_a_looooooooooooooooooong_string_value"
}
