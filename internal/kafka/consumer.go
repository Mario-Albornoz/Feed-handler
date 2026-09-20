package kafka

import (
	"context"
	"encoding/json"
	"errors"
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

// MessageReader interface for reading Kafka messages.
//
// Fetch and commit are separate on purpose. kafka.Reader.ReadMessage does both, and if
// its context expires during the commit it returns an error and the message it had
// already fetched is lost. The consumer reads with a short deadline to bound batches, so
// it fetches under the deadline and commits each batch afterwards on a context without
// one.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
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
				MinBytes: 10e3,                   // Wait for at least 10KB of data
				MaxBytes: 10e6,                   // Fetch up to 10MB per request
				MaxWait:  100 * time.Millisecond, // Don't wait longer than 100ms
			},
		),
		feedProcessor: feedProcessor,
	}
}

const (
	// batchSize is the most messages processed per batch.
	batchSize = 100
	// batchMaxWait bounds how long a partial batch waits for more messages once its
	// first message has arrived, so a slow feed is not held back until a batch fills.
	batchMaxWait = 10 * time.Millisecond
	// readErrorBackoff is the pause after a failed read, so a persistent error
	// (broker down) does not spin.
	readErrorBackoff = 100 * time.Millisecond
)

func (fc *FeedConsumer) StartReadMessageLoop(ctx context.Context) error {
	// Ticks are processed on a context that outlives shutdown: a batch that has been
	// read must be finished, and cancelling it would fail the vector writes.
	processCtx := context.WithoutCancel(ctx)

	for {
		msgs := fc.readBatch(ctx)
		fc.processBatch(processCtx, msgs)

		// Offsets are committed after the batch has been processed (at-least-once), on a
		// context that outlives shutdown and has no deadline.
		if len(msgs) > 0 {
			if err := fc.TickConsumer.CommitMessages(processCtx, msgs...); err != nil {
				log.Printf("Failed to commit %d offsets: %v", len(msgs), err)
			}
		}

		if ctx.Err() != nil {
			return nil
		}
	}
}

// readBatch blocks until at least one message has arrived, then collects whatever else
// arrives within batchMaxWait, up to batchSize. It returns what it has collected, empty
// only on shutdown.
func (fc *FeedConsumer) readBatch(ctx context.Context) []kafka.Message {
	msgs := make([]kafka.Message, 0, batchSize)

	for len(msgs) == 0 {
		m, err := fc.TickConsumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return msgs
			}
			atomic.AddUint64(&fc.ticksFailedToRead, 1)
			select {
			case <-ctx.Done():
				return msgs
			case <-time.After(readErrorBackoff):
			}
			continue
		}
		msgs = append(msgs, m)
	}

	batchCtx, cancel := context.WithTimeout(ctx, batchMaxWait)
	defer cancel()

	for len(msgs) < batchSize {
		m, err := fc.TickConsumer.FetchMessage(batchCtx)
		if err != nil {
			// Deadline (batch is full enough), shutdown, or a read error: in every case
			// hand over what has been collected.
			if batchCtx.Err() == nil && !errors.Is(err, context.Canceled) {
				atomic.AddUint64(&fc.ticksFailedToRead, 1)
			}
			break
		}
		msgs = append(msgs, m)
	}

	return msgs
}

func (fc *FeedConsumer) processBatch(ctx context.Context, msgs []kafka.Message) {
	for _, m := range msgs {
		var rawTick model.RawTick
		if err := json.Unmarshal(m.Value, &rawTick); err != nil {
			log.Printf("Malformed JSON, skipping: %v", err)
			atomic.AddUint64(&fc.ticksFailedToRead, 1)
			continue
		}

		// the partition matters to the event clock (see model.EventClock)
		if err := fc.feedProcessor.ProcessRawTicks(model.WithPartition(ctx, m.Partition), &rawTick); err != nil {
			log.Printf("Error while processing tick: %v", err)
			continue
		}
		atomic.AddUint64(&fc.ticksSucessfullyRead, 1)
	}
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
