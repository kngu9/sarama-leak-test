package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Shopify/sarama"
)

// Writer is the kafka writer object
type Writer struct {
	writer sarama.SyncProducer
	*WriterConfig
}

// WriterConfig is holds the configuration for a new writer
type WriterConfig struct {
	Topic   string
	Brokers []string
}

func (c *WriterConfig) validate() error {
	if c.Topic == "" {
		return errors.New("topic must be specified")
	}
	if len(c.Brokers) < 1 {
		return errors.New("must specified at least 1 kafka broker")
	}

	return nil
}

// NewWriter creates a new writer
func NewWriter(cfg *WriterConfig) (*Writer, error) {
	w := &Writer{}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true

	producerCfg.Producer.Partitioner = func(topic string) sarama.Partitioner {
		return sarama.NewHashPartitioner(cfg.Topic)
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, producerCfg)
	if err != nil {
		return nil, err
	}
	w.writer = producer
	w.WriterConfig = cfg

	return w, nil
}

// Write will attempt to synchronously send the kafka message
func (w *Writer) Write(ctx context.Context, key string, payload interface{}) error {
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: w.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payloadData),
	}

	errChan := make(chan error)
	defer close(errChan)

	go func() {
		_, _, err := w.writer.SendMessage(msg)
		errChan <- err
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return errors.New("context killed")
	}
}

// Close terminates the conneciton
func (w *Writer) Close() error {
	return w.writer.Close()
}
