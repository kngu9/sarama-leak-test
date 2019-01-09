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
	Topic  string
	Client sarama.Client
}

func (c *WriterConfig) validate() error {
	if c.Topic == "" {
		return errors.New("topic must be specified")
	}
	if c.Client == nil {
		return errors.New("client must be specified")
	}

	return nil
}

// NewWriter creates a new writer
func NewWriter(cfg *WriterConfig) (*Writer, error) {
	w := &Writer{}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(cfg.Client)
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

	_, _, err = w.writer.SendMessage(msg)
	return err
}

// Close terminates the conneciton
func (w *Writer) Close() error {
	return w.writer.Close()
}
