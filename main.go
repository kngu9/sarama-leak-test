package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"sarama-leak-test/pkg/kafka"
)

var (
	brokerStr = flag.String("brokers", "", "comma delimited list of kafka brokers")
	topic     = flag.String("topic", "", "kafka topic to test on")
	rounds    = flag.Int("rounds", 1000, "the amount of testing rounds per goroutine")
	routines  = flag.Int("routines", 1000, "the amount of goroutines")
)

// Writer implements the kafka writer interface
type Writer interface {
	Write(ctx context.Context, key string, payload interface{}) error
	Close() error
}

func main() {
	flag.Parse()

	if *brokerStr == "" {
		log.Fatal("must specify list of brokers")
	}
	if *topic == "" {
		log.Fatal("must specify list of topics")
	}
	if *rounds < 1 {
		log.Fatalf("invalid number of rounds: %d", *rounds)
	}
	if *routines < 1 {
		log.Fatalf("invalud number of routines: %d", *routines)
	}

	for i := 0; i < 5; i++ {
		writer, err := kafka.NewWriter(&kafka.WriterConfig{
			Brokers: strings.Split(*brokerStr, ","),
			Topic:   *topic,
		})
		if err != nil {
			log.Printf("waiting for kafka to spin up... Tries: %d/5\n", i)

			time.Sleep(time.Second * 5)
		} else {
			writer.Close()
			break
		}
	}

	writerFactory := func() (Writer, error) {
		return kafka.NewWriter(&kafka.WriterConfig{
			Brokers: strings.Split(*brokerStr, ","),
			Topic:   *topic,
		})
	}

	c := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())

	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
		os.Exit(1)
	}()

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	var wg sync.WaitGroup

	for curRoutine := 0; curRoutine < *routines; curRoutine++ {
		wg.Add(1)

		go func(curRoutine int) {
			defer wg.Done()

			log.Printf("[routine: %d] starting\n", curRoutine)

			for curRound := 0; curRound < *rounds; curRound++ {
				writer, err := writerFactory()
				if err != nil {
					log.Printf("[routine: %d] error while trying to create a writer from factory: %s\n", curRoutine, err)
					return
				}

				if err := writer.Write(ctx, "1", "hello"); err != nil {
					log.Printf("[routine: %d, round: %d] error while trying to send to kafka: %s\n", curRoutine, curRound, err)
				} else {
					log.Printf("[routine: %d, round: %d] successfully sent \n", curRoutine, curRound)
				}

				writer.Close()
			}
		}(curRoutine)
	}

	wg.Wait()
}
