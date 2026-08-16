package integration

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/segmentio/kafka-go"
)

// TickGenerator generates synthetic ticks for testing
type TickGenerator struct {
	writer *kafka.Writer
}

// NewTickGenerator creates a new synthetic tick generator
func NewTickGenerator(broker, topic string) *TickGenerator {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}

	return &TickGenerator{
		writer: writer,
	}
}

// Close closes the Kafka writer
func (g *TickGenerator) Close() error {
	return g.writer.Close()
}

// GenerateNormalTicks generates a sequence of normal market ticks
func (g *TickGenerator) GenerateNormalTicks(ctx context.Context, exchange, instrument string, count int, intervalMs int) error {
	basePrice := 100.0

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		now := time.Now()
		tick := &model.RawTick{
			ID:              instrument,
			Exchange:        exchange,
			SecType:         "E",
			ISIN:            instrument,
			LastTradedPrice: basePrice,
			TotalVolume:     float64(1000 + i*10),
			TradingTime:     now,
			Date:            now,
			Time:            now,
		}

		if err := g.writeTick(ctx, tick); err != nil {
			return err
		}

		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}

	return nil
}

// GenerateDriftTicks generates ticks with gradual price drift (Phase 1 scenario)
func (g *TickGenerator) GenerateDriftTicks(ctx context.Context, exchange, instrument string, count int, intervalMs int) error {
	basePrice := 100.0
	driftPerTick := 0.1

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		driftedPrice := basePrice + float64(i)*driftPerTick
		now := time.Now()

		tick := &model.RawTick{
			ID:              instrument,
			Exchange:        exchange,
			SecType:         "E",
			ISIN:            instrument,
			LastTradedPrice: driftedPrice,
			TotalVolume:     float64(1000 + i*10),
			TradingTime:     now,
			Date:            now,
			Time:            now,
		}

		if err := g.writeTick(ctx, tick); err != nil {
			return err
		}

		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}

	return nil
}

// GenerateInvertedQuoteTicks is no longer relevant as we don't use bid/ask
// This generates ticks with high price volatility instead
func (g *TickGenerator) GenerateInvertedQuoteTicks(ctx context.Context, exchange, instrument string, count int, intervalMs int) error {
	basePrice := 100.0

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		now := time.Now()
		// Simulate high volatility with alternating price jumps
		volatility := float64(i%2)*2.0 - 1.0 // Alternates between -1 and +1
		tick := &model.RawTick{
			ID:              instrument,
			Exchange:        exchange,
			SecType:         "E",
			ISIN:            instrument,
			LastTradedPrice: basePrice + volatility,
			TotalVolume:     float64(1000 + i*10),
			TradingTime:     now,
			Date:            now,
			Time:            now,
		}

		if err := g.writeTick(ctx, tick); err != nil {
			return err
		}

		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}

	return nil
}

// GenerateSilenceScenario generates normal ticks followed by a long silence
func (g *TickGenerator) GenerateSilenceScenario(ctx context.Context, exchange, instrument string, normalCount int, intervalMs int, silenceDurationMs int) error {
	if err := g.GenerateNormalTicks(ctx, exchange, instrument, normalCount, intervalMs); err != nil {
		return err
	}

	log.Printf("Simulating silence for %dms...", silenceDurationMs)
	time.Sleep(time.Duration(silenceDurationMs) * time.Millisecond)

	return nil
}

// writeTick serializes and writes a single tick to Kafka
func (g *TickGenerator) writeTick(ctx context.Context, tick *model.RawTick) error {
	data, err := json.Marshal(tick)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(tick.Exchange + ":" + tick.ISIN),
		Value: data,
	}

	return g.writer.WriteMessages(ctx, msg)
}
