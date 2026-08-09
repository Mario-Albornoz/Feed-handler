package kafka

import (
	"context"
	"encoding/json"
	"errors"
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
}

func (m *MockReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
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

func (m *MockReader) Close() error {
	m.closed = true
	return nil
}

// MockFeedProcessor mocks processing.FeedProcessor for testing
type MockFeedProcessor struct {
	processedTicks []*model.RawTick
	shouldFail     bool
}

func (m *MockFeedProcessor) ProcessRawTicks(ctx context.Context, tick *model.RawTick) error {
	if m.shouldFail {
		return errors.New("mock processing error")
	}
	m.processedTicks = append(m.processedTicks, tick)
	return nil
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
	
	if len(mockProcessor.processedTicks) != 1 {
		t.Fatalf("Expected 1 processed tick, got %d", len(mockProcessor.processedTicks))
	}
	
	processedTick := mockProcessor.processedTicks[0]
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
	if len(mockProcessor.processedTicks) != 0 {
		t.Errorf("Expected 0 processed ticks for malformed JSON, got %d", len(mockProcessor.processedTicks))
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
	if consumer.ticksSucessfullyRead != 0 {
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
	
	if len(mockProcessor.processedTicks) != 3 {
		t.Fatalf("Expected 3 processed ticks, got %d", len(mockProcessor.processedTicks))
	}
	
	for i, tick := range ticks {
		if mockProcessor.processedTicks[i].ID != tick.ID {
			t.Errorf("Tick %d: expected ID %s, got %s", i, tick.ID, mockProcessor.processedTicks[i].ID)
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
	
	initialSuccess := consumer.ticksSucessfullyRead
	
	go func() {
		consumer.StartReadMessageLoop(ctx)
	}()
	
	time.Sleep(50 * time.Millisecond)
	
	if consumer.ticksSucessfullyRead != initialSuccess+1 {
		t.Errorf("Expected ticksSucessfullyRead to increment by 1, got %d", consumer.ticksSucessfullyRead)
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
