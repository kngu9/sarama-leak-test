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
	"time"

	"github.com/Shopify/sarama"

	"sarama-leak-test/pkg/kafka"
)

var (
	brokerStr = flag.String("brokers", "", "comma delimited list of kafka brokers")
	topic     = flag.String("topic", "", "kafka topic to test on")
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
	if *routines < 1 {
		log.Fatalf("invalud number of routines: %d", *routines)
	}

	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true

	producerCfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return sarama.NewHashPartitioner(topic)
	}

	for i := 0; i < 5; i++ {
		client, err := sarama.NewClient(strings.Split(*brokerStr, ","), producerCfg)
		if err != nil {
			log.Printf("waiting for kafka to spin up... Tries: %d/5\n", i)

			time.Sleep(time.Second * 5)
		} else {
			client.Close()
			break
		}
	}

	writerFactory := func(client sarama.Client) (Writer, error) {
		return kafka.NewWriter(&kafka.WriterConfig{
			Client: client,
			Topic:  *topic,
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

	for curRoutine := 0; curRoutine < *routines; curRoutine++ {
		go func(curRoutine int) {
			log.Printf("[routine: %d] starting\n", curRoutine)

			client, err := sarama.NewClient(strings.Split(*brokerStr, ","), producerCfg)
			if err != nil {
				log.Printf("[routine: %d] error while trying to create new client: %s\n", curRoutine, err)
			}
			defer client.Close()

			for {
				func() {

					writer, err := writerFactory(client)
					if err != nil {
						log.Printf("[routine: %d] error while trying to create a writer from factory: %s\n", curRoutine, err)
						return
					}
					defer writer.Close()

					if err := writer.Write(ctx, "1", "hello"); err != nil {
						log.Printf("[routine: %d] error while trying to send to kafka: %s\n", curRoutine, err)
					} else {
						log.Printf("[routine: %d] successfully sent \n", curRoutine)
					}
				}()
			}
		}(curRoutine)
	}
	<-chan struct{}(nil)
}
