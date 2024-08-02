package main

import (
	"encoding/json"
	"fmt"
	"math"
	rand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"
)

func main() {
	g := &generator{
		bufSize: 10000,
		fields: map[string]field{
			"test":       int64Field{},
			"testString": stringField{length: 24},
		},
		once: &sync.Once{},

		genThreads:  1,
		sendThreads: 64,

		benthosEndpoint: "http://localhost:4195/post",
	}

	g.start()

	for {
		time.Sleep(time.Second * 5)
		fmt.Printf("buf size: %d\n", len(g.buf))

	}
}

type generator struct {
	bufSize int

	fields map[string]field
	once   *sync.Once

	buf chan string

	genThreads  int
	sendThreads int

	benthosEndpoint string
}

func (g *generator) start() {
	g.once.Do(func() {
		g.buf = make(chan string, g.bufSize)
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

					g.buf <- string(msg)

				}

			}()
		}

		for range g.sendThreads {
			go func() {
				for {

					resp, err := http.Post(
						g.benthosEndpoint,
						"application/json",
						strings.NewReader(<-g.buf),
					)

					if err != nil {
						println(err.Error())
						time.Sleep(time.Millisecond * 100)
						continue
					}

					if resp.StatusCode > 399 {
						println(resp.Status)
					}

				}
			}()
		}
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
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

	ran_str := make([]byte, f.length)

	// Generating Random string
	for i := 0; i < f.length; i++ {
		ran_str[i] = charset[rand.IntN(len(charset))]
	}

	// Displaying the random string
	return string(ran_str)
}
