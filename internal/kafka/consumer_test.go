package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

// MockReader mocks kafka.Reader for testing
type MockReader struct {
	messages      []kafka.Message
	currentIndex  int
	shouldFail    bool
	closed        bool
	contextCancel bool

	commitMu  sync.Mutex
	committed int
}

func (m *MockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.contextCancel {
		return kafka.Message{}, context.Canceled
	}

	if m.shouldFail {
		return kafka.Message{}, errors.New("mock read error")
	}

	if m.currentIndex >= len(m.messages) {
		// Simulate blocking when no more messages
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}

	msg := m.messages[m.currentIndex]
	m.currentIndex++
	return msg, nil
}

func (m *MockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.committed += len(msgs)
	return nil
}

func (m *MockReader) committedCount() int {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	return m.committed
}

func (m *MockReader) Close() error {
	m.closed = true
	return nil
}

// MockFeedProcessor mocks processing.FeedProcessor for testing
type MockFeedProcessor struct {
	mu             sync.Mutex
	processedTicks []*model.RawTick
	partitions     []int
	shouldFail     bool
}

func (m *MockFeedProcessor) ProcessRawTicks(ctx context.Context, tick *model.RawTick) error {
	if m.shouldFail {
		return errors.New("mock processing error")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processedTicks = append(m.processedTicks, tick)
	partition, _ := model.PartitionFrom(ctx)
	m.partitions = append(m.partitions, partition)
	return nil
}

// ticks returns a copy of what has been processed so far (the consumer runs in its own
// goroutine).
func (m *MockFeedProcessor) ticks() []*model.RawTick {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*model.RawTick(nil), m.processedTicks...)
}

func TestConsumer_ReadAndProcess_Success(t *testing.T) {
	tick := model.RawTick{
		ID:          "AAPL",
		Exchange:    "NYSE",
		SecType:     "E",
		Bid:         150.0,
		Ask:         150.5,
		TotalVolume: 1000,
		TradingTime: time.Now(),
	}

	tickJSON, _ := json.Marshal(tick)

	mockReader := &MockReader{
		messages: []kafka.Message{
			{Value: tickJSON},
		},
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if len(mockProcessor.ticks()) != 1 {
		t.Fatalf("Expected 1 processed tick, got %d", len(mockProcessor.ticks()))
	}

	processedTick := mockProcessor.ticks()[0]
	if processedTick.ID != tick.ID {
		t.Errorf("Expected ID %s, got %s", tick.ID, processedTick.ID)
	}
	if processedTick.Exchange != tick.Exchange {
		t.Errorf("Expected Exchange %s, got %s", tick.Exchange, processedTick.Exchange)
	}
}

func TestConsumer_MalformedJSON(t *testing.T) {
	mockReader := &MockReader{
		messages: []kafka.Message{
			{Value: []byte("invalid json")},
		},
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Should skip malformed message, not process it
	if len(mockProcessor.ticks()) != 0 {
		t.Errorf("Expected 0 processed ticks for malformed JSON, got %d", len(mockProcessor.ticks()))
	}
}

func TestConsumer_ProcessorError(t *testing.T) {
	tick := model.RawTick{
		ID:       "AAPL",
		Exchange: "NYSE",
		SecType:  "E",
	}

	tickJSON, _ := json.Marshal(tick)

	mockReader := &MockReader{
		messages: []kafka.Message{
			{Value: tickJSON},
		},
	}

	mockProcessor := &MockFeedProcessor{
		shouldFail: true,
	}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Should continue despite processing error
	if atomic.LoadUint64(&consumer.ticksSucessfullyRead) != 0 {
		t.Errorf("Expected 0 successful reads when processor fails")
	}
}

func TestConsumer_ContextCancellation(t *testing.T) {
	mockReader := &MockReader{
		contextCancel: true,
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := consumer.StartReadMessageLoop(ctx)

	// Should return nil when context is canceled (intentional shutdown)
	if err != nil {
		t.Errorf("Expected nil error on context cancellation, got %v", err)
	}
}

func TestConsumer_MultipleMessages(t *testing.T) {
	ticks := []model.RawTick{
		{ID: "AAPL", Exchange: "NYSE", SecType: "E"},
		{ID: "GOOGL", Exchange: "NASDAQ", SecType: "E"},
		{ID: "MSFT", Exchange: "NASDAQ", SecType: "E"},
	}

	messages := make([]kafka.Message, len(ticks))
	for i, tick := range ticks {
		tickJSON, _ := json.Marshal(tick)
		messages[i] = kafka.Message{Value: tickJSON}
	}

	mockReader := &MockReader{
		messages: messages,
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()

	time.Sleep(150 * time.Millisecond)

	if len(mockProcessor.ticks()) != 3 {
		t.Fatalf("Expected 3 processed ticks, got %d", len(mockProcessor.ticks()))
	}

	for i, tick := range ticks {
		if mockProcessor.ticks()[i].ID != tick.ID {
			t.Errorf("Tick %d: expected ID %s, got %s", i, tick.ID, mockProcessor.ticks()[i].ID)
		}
	}
}

func TestConsumer_Metrics(t *testing.T) {
	tick := model.RawTick{
		ID:       "AAPL",
		Exchange: "NYSE",
		SecType:  "E",
	}

	tickJSON, _ := json.Marshal(tick)

	mockReader := &MockReader{
		messages: []kafka.Message{
			{Value: tickJSON},
		},
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	initialSuccess := atomic.LoadUint64(&consumer.ticksSucessfullyRead)

	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if got := atomic.LoadUint64(&consumer.ticksSucessfullyRead); got != initialSuccess+1 {
		t.Errorf("Expected ticksSucessfullyRead to increment by 1, got %d", got)
	}
}

func TestConsumer_Close(t *testing.T) {
	mockReader := &MockReader{}
	mockProcessor := &MockFeedProcessor{}

	consumer := &FeedConsumer{
		TickConsumer:  mockReader,
		feedProcessor: mockProcessor,
	}

	err := consumer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !mockReader.closed {
		t.Error("Expected reader to be closed")
	}
}

func TestNewFeedConsumer(t *testing.T) {
	cfg := config.KafkaConfig{
		Brokers:       []string{"localhost:9092"},
		InputTopic:    "test-topic",
		ConsumerGroup: "test-group",
	}

	mockProcessor := &MockFeedProcessor{}

	consumer := NewFeedConsumer(cfg, mockProcessor)

	if consumer == nil {
		t.Fatal("NewFeedConsumer returned nil")
	}

	if consumer.feedProcessor == nil {
		t.Error("feedProcessor not set in constructor")
	}

	if consumer.TickConsumer == nil {
		t.Error("TickConsumer not set in constructor")
	}
}

func tickMessages(t *testing.T, n int) []kafka.Message {
	t.Helper()
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		b, _ := json.Marshal(model.RawTick{ID: "T", Exchange: "NYSE", SecType: "E"})
		msgs[i] = kafka.Message{Value: b}
	}
	return msgs
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestConsumer_ProcessesMoreThanOneBatch(t *testing.T) {
	proc := &MockFeedProcessor{}
	consumer := &FeedConsumer{TickConsumer: &MockReader{messages: tickMessages(t, 250)}, feedProcessor: proc}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.StartReadMessageLoop(ctx)

	waitFor(t, "250 ticks", func() bool { return len(proc.ticks()) == 250 })
}

// A slow feed must not be held back until 100 messages have arrived.
func TestConsumer_PartialBatchIsNotHeldBack(t *testing.T) {
	proc := &MockFeedProcessor{}
	consumer := &FeedConsumer{TickConsumer: &MockReader{messages: tickMessages(t, 7)}, feedProcessor: proc}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.StartReadMessageLoop(ctx)

	waitFor(t, "7 ticks", func() bool { return len(proc.ticks()) == 7 })
}

// cancellingReader yields its messages and then shuts the consumer down mid-batch.
type cancellingReader struct {
	messages []kafka.Message
	cancel   context.CancelFunc
	index    int
}

func (r *cancellingReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if r.index < len(r.messages) {
		m := r.messages[r.index]
		r.index++
		return m, nil
	}
	r.cancel()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *cancellingReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (r *cancellingReader) Close() error { return nil }

// On shutdown the messages already read must still be processed, not dropped.
func TestConsumer_ShutdownProcessesCollectedMessages(t *testing.T) {
	proc := &MockFeedProcessor{}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &FeedConsumer{
		TickConsumer:  &cancellingReader{messages: tickMessages(t, 3), cancel: cancel},
		feedProcessor: proc,
	}

	done := make(chan error, 1)
	go func() { done <- consumer.StartReadMessageLoop(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil on shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop")
	}

	if got := len(proc.ticks()); got != 3 {
		t.Errorf("expected the 3 collected messages to be processed on shutdown, got %d", got)
	}
}

type countingFailingReader struct{ calls int32 }

func (r *countingFailingReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	atomic.AddInt32(&r.calls, 1)
	return kafka.Message{}, errors.New("broker down")
}

func (r *countingFailingReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (r *countingFailingReader) Close() error { return nil }

// A persistent read error must back off rather than spin.
func TestConsumer_ReadErrorsDoNotSpin(t *testing.T) {
	reader := &countingFailingReader{}
	consumer := &FeedConsumer{TickConsumer: reader, feedProcessor: &MockFeedProcessor{}}

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	consumer.StartReadMessageLoop(ctx)

	if calls := atomic.LoadInt32(&reader.calls); calls > 10 {
		t.Errorf("consumer spun on read errors: %d calls in 350ms", calls)
	}
	if _, failed := consumer.GetMetrics(); failed == 0 {
		t.Error("read errors should be counted")
	}
}

// kafka.Reader.ReadMessage fetches and commits, and drops the message if its context
// expires during the commit. The consumer fetches under a short deadline and commits
// afterwards, so a slow commit must not lose messages.
type slowCommitReader struct {
	MockReader
	delay time.Duration
}

func (r *slowCommitReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		return ctx.Err() // a commit on a context with a deadline would fail here
	}
	return r.MockReader.CommitMessages(ctx, msgs...)
}

func TestConsumer_SlowCommitsDoNotLoseMessages(t *testing.T) {
	reader := &slowCommitReader{MockReader: MockReader{messages: tickMessages(t, 300)}, delay: 60 * time.Millisecond}
	proc := &MockFeedProcessor{}
	consumer := &FeedConsumer{TickConsumer: reader, feedProcessor: proc}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.StartReadMessageLoop(ctx)

	waitFor(t, "300 ticks despite 60 ms commits (the batch deadline is 10 ms)", func() bool { return len(proc.ticks()) == 300 })
	waitFor(t, "all offsets committed", func() bool { return reader.committedCount() == 300 })
}

func TestConsumer_CommitsEveryProcessedMessage(t *testing.T) {
	reader := &MockReader{messages: tickMessages(t, 42)}
	proc := &MockFeedProcessor{}
	consumer := &FeedConsumer{TickConsumer: reader, feedProcessor: proc}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.StartReadMessageLoop(ctx)

	waitFor(t, "42 ticks", func() bool { return len(proc.ticks()) == 42 })
	waitFor(t, "42 commits", func() bool { return reader.committedCount() == 42 })
}

// The event clock needs to know which partition a message came from.
func TestConsumer_PassesThePartitionToTheProcessor(t *testing.T) {
	msgs := tickMessages(t, 3)
	msgs[0].Partition, msgs[1].Partition, msgs[2].Partition = 2, 0, 1
	proc := &MockFeedProcessor{}
	consumer := &FeedConsumer{TickConsumer: &MockReader{messages: msgs}, feedProcessor: proc}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.StartReadMessageLoop(ctx)

	waitFor(t, "3 ticks", func() bool { return len(proc.ticks()) == 3 })
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.partitions) != 3 || proc.partitions[0] != 2 || proc.partitions[1] != 0 || proc.partitions[2] != 1 {
		t.Errorf("partitions seen by the processor: %v, want [2 0 1]", proc.partitions)
	}
}
