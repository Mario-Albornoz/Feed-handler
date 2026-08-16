package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

// TickProcessor interface for processing raw ticks
type TickProcessor interface {
	ProcessRawTicks(ctx context.Context, tick *model.RawTick) error
}

// MessageReader interface for reading Kafka messages
type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

type FeedConsumer struct {
	TickConsumer  MessageReader
	feedProcessor TickProcessor

	ticksSucessfullyRead uint64
	ticksFailedToRead    uint64
}

func NewFeedConsumer(kafkaConfig config.KafkaConfig, feedProcessor TickProcessor) *FeedConsumer {
	return &FeedConsumer{
		TickConsumer: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  kafkaConfig.Brokers,
				Topic:    kafkaConfig.InputTopic,
				GroupID:  kafkaConfig.ConsumerGroup,
				MaxBytes: 10e6,
				MaxWait:  100 * time.Millisecond, // Don't wait longer than 100ms
			},
		),
		feedProcessor: feedProcessor,
	}
}

func (fc *FeedConsumer) StartReadMessageLoop(ctx context.Context) error {
	msgs := make([]kafka.Message, 0, 100)
	for i := 0; i < 100; i++ {
		m, err := fc.TickConsumer.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				break
			}
			log.Printf("Error occurred trying to read message %v", m)
			atomic.AddUint64(&fc.ticksFailedToRead, 1)
			continue
		}
		msgs = append(msgs, m)
	}

	for _, m := range msgs {
		var rawTick model.RawTick
		err := json.Unmarshal(m.Value, &rawTick)
		if err != nil {
			log.Printf("Malformed JSON, skipping: %v", err)
			continue
		}

		err = fc.feedProcessor.ProcessRawTicks(ctx, &rawTick)
		if err != nil {
			log.Printf("Error while processing tick: %v", err)
			continue
		}
		atomic.AddUint64(&fc.ticksSucessfullyRead, 1)
	}
	return nil
}

func (fc *FeedConsumer) Close() error {
	if err := fc.TickConsumer.Close(); err != nil {
		log.Printf("Error closing feed consumer: %v", err)
		return err
	}
	return nil
}

func (fc *FeedConsumer) GetMetrics() (successfulReads, failedReads uint64) {
	return atomic.LoadUint64(&fc.ticksSucessfullyRead), atomic.LoadUint64(&fc.ticksFailedToRead)
}
