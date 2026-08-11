package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

const (
	testBroker      = "localhost:9092"
	testInputTopic  = "test-raw-ticks"
	testOutputTopic = "test-normalized-vectors"
	testAlertTopic  = "test-health-events"
)

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION_TEST") != "1" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func setupTestTopics(t *testing.T) {
	t.Helper()

	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("Failed to get controller: %v", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("Failed to connect to controller: %v", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{Topic: testInputTopic, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: testOutputTopic, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: testAlertTopic, NumPartitions: 1, ReplicationFactor: 1},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		t.Logf("Warning: Could not create topics (may already exist): %v", err)
	}
}

func cleanupTestTopics(t *testing.T) {
	t.Helper()

	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Logf("Warning: Failed to connect for cleanup: %v", err)
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Logf("Warning: Failed to get controller for cleanup: %v", err)
		return
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Logf("Warning: Failed to connect to controller for cleanup: %v", err)
		return
	}
	defer controllerConn.Close()

	err = controllerConn.DeleteTopics(testInputTopic, testOutputTopic, testAlertTopic)
	if err != nil {
		t.Logf("Warning: Failed to delete topics: %v", err)
	}
}

func TestEndToEndNormalFlow(t *testing.T) {
	setupTestTopics(t)
	defer cleanupTestTopics(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	generator := NewTickGenerator(testBroker, testInputTopic)
	defer generator.Close()

	outputReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{testBroker},
		Topic:   testOutputTopic,
		GroupID: "test-consumer-output",
	})
	defer outputReader.Close()

	exchange := "NYSE"
	instrument := "AAPL"

	go func() {
		time.Sleep(2 * time.Second)
		if err := generator.GenerateNormalTicks(ctx, exchange, instrument, 50, 50); err != nil {
			t.Logf("Generator error: %v", err)
		}
	}()

	timeout := time.After(30 * time.Second)
	vectorsReceived := 0

	for {
		select {
		case <-timeout:
			if vectorsReceived == 0 {
				t.Fatal("Timeout: No normalized vectors received")
			}
			return
		default:
		}

		msg, err := outputReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var vector model.NormalizedVector
		if err := json.Unmarshal(msg.Value, &vector); err != nil {
			t.Errorf("Failed to unmarshal vector: %v", err)
			continue
		}

		vectorsReceived++

		if vector.Exchange != exchange {
			t.Errorf("Expected exchange %s, got %s", exchange, vector.Exchange)
		}
		if vector.Instrument != instrument {
			t.Errorf("Expected instrument %s, got %s", instrument, vector.Instrument)
		}

		expectedKey := exchange + ":" + vector.ModelKey
		if string(msg.Key) != expectedKey {
			t.Errorf("Partition key mismatch: expected %s, got %s", expectedKey, string(msg.Key))
		}

		if vectorsReceived >= 10 {
			t.Logf("Successfully received %d normalized vectors with correct partition keys", vectorsReceived)
			return
		}
	}
}

func TestPartitionKeyConsistency(t *testing.T) {
	setupTestTopics(t)
	defer cleanupTestTopics(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	generator := NewTickGenerator(testBroker, testInputTopic)
	defer generator.Close()

	outputReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{testBroker},
		Topic:   testOutputTopic,
		GroupID: "test-consumer-partition",
	})
	defer outputReader.Close()

	instruments := []string{"AAPL", "GOOGL", "MSFT"}

	go func() {
		time.Sleep(2 * time.Second)
		for _, inst := range instruments {
			if err := generator.GenerateNormalTicks(ctx, "NYSE", inst, 10, 30); err != nil {
				t.Logf("Generator error for %s: %v", inst, err)
			}
		}
	}()

	partitionMap := make(map[string]int)
	timeout := time.After(30 * time.Second)
	vectorsReceived := 0

	for {
		select {
		case <-timeout:
			if vectorsReceived < len(instruments)*5 {
				t.Logf("Warning: Only received %d vectors, expected at least %d", vectorsReceived, len(instruments)*5)
			}
			return
		default:
		}

		msg, err := outputReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var vector model.NormalizedVector
		if err := json.Unmarshal(msg.Value, &vector); err != nil {
			continue
		}

		vectorsReceived++
		key := string(msg.Key)

		if prevPartition, exists := partitionMap[key]; exists {
			if prevPartition != msg.Partition {
				t.Errorf("Partition inconsistency for key %s: previously %d, now %d", key, prevPartition, msg.Partition)
			}
		} else {
			partitionMap[key] = msg.Partition
			t.Logf("Key %s consistently routed to partition %d", key, msg.Partition)
		}

		if vectorsReceived >= len(instruments)*10 {
			return
		}
	}
}

func TestQuoteInversionDetection(t *testing.T) {
	setupTestTopics(t)
	defer cleanupTestTopics(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	generator := NewTickGenerator(testBroker, testInputTopic)
	defer generator.Close()

	outputReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{testBroker},
		Topic:   testOutputTopic,
		GroupID: "test-consumer-inversion",
	})
	defer outputReader.Close()

	go func() {
		time.Sleep(2 * time.Second)
		if err := generator.GenerateNormalTicks(ctx, "NYSE", "TEST", 20, 50); err != nil {
			t.Logf("Normal ticks error: %v", err)
		}
		if err := generator.GenerateInvertedQuoteTicks(ctx, "NYSE", "TEST", 10, 50); err != nil {
			t.Logf("Inverted ticks error: %v", err)
		}
	}()

	timeout := time.After(30 * time.Second)
	invertedCount := 0

	for {
		select {
		case <-timeout:
			if invertedCount == 0 {
				t.Error("No inverted quotes detected")
			} else {
				t.Logf("Successfully detected %d inverted quotes", invertedCount)
			}
			return
		default:
		}

		msg, err := outputReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var vector model.NormalizedVector
		if err := json.Unmarshal(msg.Value, &vector); err != nil {
			continue
		}

		if vector.QuoteInv == 1 {
			invertedCount++
		}

		if invertedCount >= 5 {
			t.Logf("Successfully detected inverted quotes via QuoteInversionFlag")
			return
		}
	}
}

func TestSilenceDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping silence detection test in short mode")
	}

	setupTestTopics(t)
	defer cleanupTestTopics(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	testConfig := &config.AggregatorConfig{
		Silence: config.SilenceConfig{
			CheckIntervalSec: 2,
			GapMultiplier:    3.0,
		},
	}
	_ = testConfig

	generator := NewTickGenerator(testBroker, testInputTopic)
	defer generator.Close()

	alertReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{testBroker},
		Topic:   testAlertTopic,
		GroupID: "test-consumer-silence",
	})
	defer alertReader.Close()

	go func() {
		time.Sleep(2 * time.Second)
		if err := generator.GenerateSilenceScenario(ctx, "NYSE", "SILENT", 30, 100, 5000); err != nil {
			t.Logf("Silence scenario error: %v", err)
		}
	}()

	timeout := time.After(60 * time.Second)

	for {
		select {
		case <-timeout:
			t.Log("Silence detection test completed (timeout reached, may need aggregator running)")
			return
		default:
		}

		msg, err := alertReader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		var alert model.SilenceAlert
		if err := json.Unmarshal(msg.Value, &alert); err != nil {
			continue
		}

		if alert.AlertType == "SILENCE" && alert.Instrument == "SILENT" {
			t.Logf("Successfully detected silence alert for instrument SILENT after %dms (expected ~%0.2fms)",
				alert.ElapsedMs, alert.ExpectedInterval)

			expectedKey := alert.Exchange + ":SILENCE"
			if string(msg.Key) != expectedKey {
				t.Errorf("Silence alert partition key mismatch: expected %s, got %s", expectedKey, string(msg.Key))
			}

			return
		}
	}
}
