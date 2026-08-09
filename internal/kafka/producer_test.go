package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

// MockWriter mocks kafka.Writer for testing
type MockWriter struct {
	messages      []kafka.Message
	shouldFail    bool
	failCount     int
	currentAttempt int
}

func (m *MockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.currentAttempt++
	if m.shouldFail && m.currentAttempt <= m.failCount {
		return errors.New("mock write error")
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *MockWriter) Close() error {
	return nil
}

func TestProducer_WriteVector_Success(t *testing.T) {
	mockWriter := &MockWriter{}
	producer := &Producer{
		vectorProducer: mockWriter,
	}

	vector := &model.NormalizedVector{
		Timestamp:      time.Now(),
		Exchange:       "NYSE",
		Instrument:     "AAPL",
		Class:          "E",
		ModelKey:       "equity",
		ZIntertickFast: 1.5,
		ZSpreadFast:    0.3,
		GapFlag:        0,
		QuoteInv:       0,
	}

	err := producer.WriteVector(context.Background(), vector)
	if err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	if len(mockWriter.messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(mockWriter.messages))
	}

	msg := mockWriter.messages[0]

	// Verify partition key
	expectedKey := "NYSE:equity"
	if string(msg.Key) != expectedKey {
		t.Errorf("Expected key %s, got %s", expectedKey, string(msg.Key))
	}

	// Verify message can be deserialized
	var decoded model.NormalizedVector
	err = json.Unmarshal(msg.Value, &decoded)
	if err != nil {
		t.Fatalf("Failed to deserialize message: %v", err)
	}

	if decoded.Exchange != vector.Exchange {
		t.Errorf("Expected exchange %s, got %s", vector.Exchange, decoded.Exchange)
	}
	if decoded.ModelKey != vector.ModelKey {
		t.Errorf("Expected modelKey %s, got %s", vector.ModelKey, decoded.ModelKey)
	}
}

func TestProducer_WriteAlert_Success(t *testing.T) {
	mockWriter := &MockWriter{}
	producer := &Producer{
		alertProducer: mockWriter,
	}

	alert := &model.SilenceAlert{
		InstrumentIdentifier: "AAPL",
		Exchange:             "NYSE",
		LatencyLevel:         3,
	}

	err := producer.WriteAlert(context.Background(), alert)
	if err != nil {
		t.Fatalf("WriteAlert failed: %v", err)
	}

	if len(mockWriter.messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(mockWriter.messages))
	}

	msg := mockWriter.messages[0]

	// Verify partition key for alerts
	expectedKey := "NYSE:SILENCE"
	if string(msg.Key) != expectedKey {
		t.Errorf("Expected key %s, got %s", expectedKey, string(msg.Key))
	}

	// Verify message can be deserialized
	var decoded model.SilenceAlert
	err = json.Unmarshal(msg.Value, &decoded)
	if err != nil {
		t.Fatalf("Failed to deserialize alert: %v", err)
	}

	if decoded.Exchange != alert.Exchange {
		t.Errorf("Expected exchange %s, got %s", alert.Exchange, decoded.Exchange)
	}
}

func TestProducer_WriteVector_PartitionKeys(t *testing.T) {
	tests := []struct {
		name       string
		exchange   string
		modelKey   string
		expectedKey string
	}{
		{"NYSE Equity", "NYSE", "equity", "NYSE:equity"},
		{"NASDAQ Equity", "NASDAQ", "equity", "NASDAQ:equity"},
		{"NYSE Index", "NYSE", "index", "NYSE:index"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockWriter := &MockWriter{}
			producer := &Producer{
				vectorProducer: mockWriter,
			}

			vector := &model.NormalizedVector{
				Exchange: tt.exchange,
				ModelKey: tt.modelKey,
			}

			err := producer.WriteVector(context.Background(), vector)
			if err != nil {
				t.Fatalf("WriteVector failed: %v", err)
			}

			if string(mockWriter.messages[0].Key) != tt.expectedKey {
				t.Errorf("Expected key %s, got %s", tt.expectedKey, string(mockWriter.messages[0].Key))
			}
		})
	}
}

func TestProducer_WriteAlert_PartitionKeys(t *testing.T) {
	tests := []struct {
		name         string
		exchange     string
		expectedKey  string
	}{
		{"NYSE Alert", "NYSE", "NYSE:SILENCE"},
		{"NASDAQ Alert", "NASDAQ", "NASDAQ:SILENCE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockWriter := &MockWriter{}
			producer := &Producer{
				alertProducer: mockWriter,
			}

			alert := &model.SilenceAlert{
				Exchange: tt.exchange,
			}

			err := producer.WriteAlert(context.Background(), alert)
			if err != nil {
				t.Fatalf("WriteAlert failed: %v", err)
			}

			if string(mockWriter.messages[0].Key) != tt.expectedKey {
				t.Errorf("Expected key %s, got %s", tt.expectedKey, string(mockWriter.messages[0].Key))
			}
		})
	}
}

func TestProducer_WriteVector_Error(t *testing.T) {
	mockWriter := &MockWriter{
		shouldFail: true,
		failCount:  999, // Always fail
	}
	producer := &Producer{
		vectorProducer: mockWriter,
	}

	vector := &model.NormalizedVector{
		Exchange: "NYSE",
		ModelKey: "equity",
	}

	err := producer.WriteVector(context.Background(), vector)
	if err == nil {
		t.Fatal("Expected error when write fails, got nil")
	}

	if producer.vectorsFailed != 1 {
		t.Errorf("Expected vectorsFailed=1, got %d", producer.vectorsFailed)
	}
}

func TestProducer_Close(t *testing.T) {
	vectorWriter := &MockWriter{}
	alertWriter := &MockWriter{}
	
	producer := &Producer{
		vectorProducer: vectorWriter,
		alertProducer:  alertWriter,
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestProducer_Metrics(t *testing.T) {
	mockWriter := &MockWriter{}
	producer := &Producer{
		vectorProducer: mockWriter,
	}

	initialSuccess := producer.vectorsWritten
	
	vector := &model.NormalizedVector{
		Exchange: "NYSE",
		ModelKey: "equity",
	}

	err := producer.WriteVector(context.Background(), vector)
	if err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	if producer.vectorsWritten != initialSuccess+1 {
		t.Errorf("Expected vectorsWritten to increment by 1")
	}
}
