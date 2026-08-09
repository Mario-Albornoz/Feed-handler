package kafka

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/processing"
	"github.com/segmentio/kafka-go"
)

type FeedConsumer struct {
	TickConsumer  *kafka.Reader
	feedProcessor *processing.FeedProcessor

	ticksSucessfullyRead uint64
	ticksFailedToRead    uint64
}

func NewFeedConsumer(kafkaConfig config.KafkaConfig, feedProcessor *processing.FeedProcessor) *FeedConsumer {
	return &FeedConsumer{
		TickConsumer: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:   kafkaConfig.Brokers,
				Topic:     kafkaConfig.InputTopic,
				Partition: 0,
				MaxBytes:  10e6,
			},
		),
	}
}

func (fc *FeedConsumer) startReadMessagedLopp() {
	for {
		m, err := fc.TickConsumer.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error occurred trying to read message %v", m)
			atomic.AddUint64(&fc.ticksFailedToRead, 1)
		}
		atomic.AddUint64(&fc.ticksSucessfullyRead, 1)
	}

}

func (fc *FeedConsumer) Close() error {
	if err := fc.TickConsumer.Close(); err != nil {
		log.Printf("Error Closing feed consumer", err)
		return err
	}
	return nil
}
