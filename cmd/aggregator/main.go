package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

const (
	defaultConfigPath   = "config/aggregator.yaml"
	defaultRegistryPath = "data/registry.gob"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting feed-handler-aggregator...")

	system, err := NewSystemBuilder().
		WithConfigPath(getConfigPath()).
		WithConfig().
		WithSessionResolver().
		WithRegistry().
		WithTopics().
		WithProducer().
		WithEventClock().
		WithAlertLogs().
		WithDetector().
		WithValidator().
		WithProcessor().
		WithConsumer().
		WithThroughputTracker().
		Build()

	if err != nil {
		log.Fatalf("Failed to build system: %v", err)
	}

	defer system.Shutdown()

	if err := system.Run(context.Background()); err != nil {
		log.Printf("System error: %v", err)
	}
}

func getConfigPath() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	if envPath := os.Getenv("AGGREGATOR_CONFIG"); envPath != "" {
		return envPath
	}
	return defaultConfigPath
}

func getRegistryPath() string {
	if envPath := os.Getenv("REGISTRY_PATH"); envPath != "" {
		return envPath
	}
	return defaultRegistryPath
}

func logConfiguration(cfg *config.AggregatorConfig) {
	log.Println("Configuration loaded successfully")
	log.Printf("  Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("  Fast window: %.0f ticks, Slow window: %.0f ticks",
		cfg.Windows.FastWindowTicks, cfg.Windows.SlowWindowTicks)
	log.Printf("  CUSUM slack: %.2f, threshold: %.2f", cfg.CUSUM.Slack, cfg.CUSUM.Threshold)
	log.Printf("  Silence: %.4g quantile of the instrument's own gaps x %.2f, min %d observations, scan every %ds",
		cfg.Silence.GapQuantile, cfg.Silence.GapQuantileMultiplier, cfg.Silence.MinObservations, cfg.Silence.CheckIntervalSec)
	log.Printf("  Validation enabled: %t (timestamp tolerance: %dms)",
		cfg.Validation.Enabled, cfg.Validation.TimestampToleranceMs)
	log.Printf("  Alert logs: silence=%q validation=%q, silence alerts to kafka: %t",
		cfg.Alerts.SilenceLog, cfg.Alerts.ValidationLog, cfg.Alerts.KafkaSilenceEnabled())
}

func buildSessionResolver(cfg *config.AggregatorConfig) (*model.SessionResolver, error) {
	// Parse default exchange hours
	defaultHours, err := parseExchangeHours("default", cfg.DefaultExchangeInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse default exchange: %w", err)
	}

	// Parse specific exchange overrides if present
	exchanges := make(map[string]*model.ExchangeHours)
	for exchangeName, exchangeInfo := range cfg.ExchangeInfo {
		hours, err := parseExchangeHours(exchangeName, exchangeInfo)
		if err != nil {
			return nil, err
		}
		exchanges[exchangeName] = hours
	}

	return model.NewSessionResolverWithDefault(defaultHours, exchanges), nil
}

func parseExchangeHours(name string, info config.ExchangeInfo) (*model.ExchangeHours, error) {
	tz, err := config.ParseTimezone(info.Timezone)
	if err != nil {
		return nil, fmt.Errorf("exchange %s: %w", name, err)
	}

	times, err := parseExchangeTimes(name, info)
	if err != nil {
		return nil, err
	}

	weekdays, err := config.ParseWeekdays(info.TradingWeekdays)
	if err != nil {
		return nil, fmt.Errorf("exchange %s: %w", name, err)
	}

	return &model.ExchangeHours{
		Timezone:        tz,
		PreMarketStart:  times["premarket"],
		MarketOpen:      times["open"],
		MiddayStart:     times["midday"],
		CloseStart:      times["close"],
		MarketClose:     times["market_close"],
		AfterHoursEnd:   times["afterhours"],
		TradingWeekdays: weekdays,
	}, nil
}

func parseExchangeTimes(name string, info config.ExchangeInfo) (map[string]time.Duration, error) {
	times := map[string]string{
		"premarket":    info.PreMarketStart,
		"open":         info.MarketOpen,
		"midday":       info.MiddayStart,
		"close":        info.CloseStart,
		"market_close": info.MarketClose,
		"afterhours":   info.AfterHoursEnd,
	}

	parsed := make(map[string]time.Duration)
	for key, value := range times {
		duration, err := config.ParseTimeOfDay(value)
		if err != nil {
			return nil, fmt.Errorf("exchange %s %s: %w", name, key, err)
		}
		parsed[key] = duration
	}

	return parsed, nil
}
