// Package kafka contains all producers and consumers for this project
// TODO we might need another type of write functions for alerts regarding the exchange itself not only the instruments
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

// MessageWriter interface for writing Kafka messages
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Producer struct {
	vectorProducer MessageWriter
	alertProducer  MessageWriter

	vectorsWritten uint64
	alertsWritten  uint64
	vectorsFailed  uint64
	alertsFailed   uint64
}

func NewProducer(vectorTopicName string, alertsTopicName string, kafkaURL string) *Producer {
	return &Producer{
		vectorProducer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaURL),
			Topic:        vectorTopicName,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 15 * time.Millisecond,
			BatchSize:    500,
			RequiredAcks: kafka.RequireOne,
			WriteTimeout: 2 * time.Second,
			MaxAttempts:  3,
			Compression:  kafka.Snappy,
		},
		alertProducer: &kafka.Writer{
			Addr:         kafka.TCP(kafkaURL),
			Topic:        alertsTopicName,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 5 * time.Millisecond,
			BatchSize:    10,
			RequiredAcks: kafka.RequireOne,
			WriteTimeout: 2 * time.Second,
			MaxAttempts:  3,
		},
	}
}

func (p *Producer) WriteVector(ctx context.Context, vector *model.NormalizedVector) error {

	jsonMessage, err := json.Marshal(vector)
	if err != nil {
		log.Printf("Error occured during json serialization for vector struct %v", vector)
		atomic.AddUint64(&p.vectorsFailed, 1)
		return err
	}

	err = p.vectorProducer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(vector.Exchange + ":" + vector.ModelKey),
		Value: jsonMessage,
	})
	if err == nil {
		atomic.AddUint64(&p.vectorsWritten, 1)
		return nil
	}

	atomic.AddUint64(&p.vectorsFailed, 1)
	log.Printf("dropped vector: instrument=%s error=%v", vector.Instrument, err)
	return fmt.Errorf("writing vector for %s: %w", vector.Instrument, err)
}

func (p *Producer) WriteAlert(ctx context.Context, alert *model.SilenceAlert) error {

	jsonMessage, err := json.Marshal(alert)
	if err != nil {
		log.Printf("Error occured during json serialization for vector alert %v", alert)
		atomic.AddUint64(&p.alertsFailed, 1)
		return err
	}

	err = p.alertProducer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(alert.Exchange + ":SILENCE"),
		Value: jsonMessage,
	})
	if err == nil {
		atomic.AddUint64(&p.alertsWritten, 1)
		return nil
	}

	atomic.AddUint64(&p.alertsFailed, 1)
	log.Printf("dropped alert: instrument=%s error=%v", alert.Instrument, err)
	return fmt.Errorf("writing alert for %s: %w", alert.Instrument, err)
}

func (p *Producer) Close() error {
	var errs []error
	if err := p.vectorProducer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing vector producer: %w", err))
	}
	if err := p.alertProducer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing alert producer: %w", err))
	}
	return errors.Join(errs...)
}
