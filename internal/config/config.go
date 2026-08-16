// Package config includes the Configuration for feed-handler
// inlucdes all thresholds, kafka configuration, instrument profile configuration
// consurems the infromation through yaml file
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type AggregatorConfig struct {
	Kafka               KafkaConfig             `yaml:"kafka"`
	Windows             WindowConfig            `yaml:"windows"`
	CUSUM               CUSUMConfig             `yaml:"cusum"`
	Silence             SilenceConfig           `yaml:"silence"`
	Profiles            map[string]ClassProfile `yaml:"profiles"`
	DefaultExchangeInfo ExchangeInfo            `yaml:"default_exchange"`
	ExchangeInfo        map[string]ExchangeInfo `yaml:"exchanges,omitempty"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	InputTopic    string   `yaml:"input_topic"`
	OutputTopic   string   `yaml:"output_topic"`
	AlertTopic    string   `yaml:"alert_topic"`
	ConsumerGroup string   `yaml:"consumer_group"`
}

type WindowConfig struct {
	FastWindowTicks float64 `yaml:"fast_window_ticks"`
	SlowWindowTicks float64 `yaml:"slow_window_ticks"`
}

type CUSUMConfig struct {
	Slack     float64 `yaml:"slack"`
	Threshold float64 `yaml:"threshold"`
}

type SilenceConfig struct {
	CheckIntervalSec int     `yaml:"check_interval_sec"`
	GapMultiplier    float64 `yaml:"gap_multiplier"`
}

type ClassProfile struct {
	ModelKey string `yaml:"model_key"`
}

type ExchangeInfo struct {
	Timezone        string   `yaml:"timezone"`
	PreMarketStart  string   `yaml:"premarket_start"`
	MarketOpen      string   `yaml:"market_open"`
	MiddayStart     string   `yaml:"midday_start"`
	CloseStart      string   `yaml:"close_start"`
	MarketClose     string   `yaml:"market_close"`
	AfterHoursEnd   string   `yaml:"afterhours_end"`
	TradingWeekdays []string `yaml:"trading_weekdays"`
}

func Load(path string) (*AggregatorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg AggregatorConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// Validate checks if the loaded config is valid
func (cfg *AggregatorConfig) Validate() error {
	// Validate Kafka config
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers cannot be empty")
	}
	if cfg.Kafka.InputTopic == "" {
		return fmt.Errorf("kafka input_topic cannot be empty")
	}
	if cfg.Kafka.OutputTopic == "" {
		return fmt.Errorf("kafka output_topic cannot be empty")
	}
	if cfg.Kafka.AlertTopic == "" {
		return fmt.Errorf("kafka alert_topic cannot be empty")
	}
	if cfg.Kafka.ConsumerGroup == "" {
		return fmt.Errorf("kafka consumer_group cannot be empty")
	}

	// Validate Windows config
	if cfg.Windows.FastWindowTicks <= 0 {
		return fmt.Errorf("fast_window_ticks must be positive, got %f", cfg.Windows.FastWindowTicks)
	}
	if cfg.Windows.SlowWindowTicks <= 0 {
		return fmt.Errorf("slow_window_ticks must be positive, got %f", cfg.Windows.SlowWindowTicks)
	}
	if cfg.Windows.FastWindowTicks >= cfg.Windows.SlowWindowTicks {
		return fmt.Errorf("fast_window_ticks (%f) must be less than slow_window_ticks (%f)",
			cfg.Windows.FastWindowTicks, cfg.Windows.SlowWindowTicks)
	}

	// Validate CUSUM config
	if cfg.CUSUM.Slack < 0 {
		return fmt.Errorf("cusum slack must be non-negative, got %f", cfg.CUSUM.Slack)
	}
	if cfg.CUSUM.Threshold <= 0 {
		return fmt.Errorf("cusum threshold must be positive, got %f", cfg.CUSUM.Threshold)
	}

	// Validate Silence config
	if cfg.Silence.CheckIntervalSec <= 0 {
		return fmt.Errorf("silence check_interval_sec must be positive, got %d", cfg.Silence.CheckIntervalSec)
	}
	if cfg.Silence.GapMultiplier <= 0 {
		return fmt.Errorf("silence gap_multiplier must be positive, got %f", cfg.Silence.GapMultiplier)
	}

	// Validate Profiles
	if len(cfg.Profiles) == 0 {
		return fmt.Errorf("at least one instrument profile must be defined")
	}
	for name, profile := range cfg.Profiles {
		if profile.ModelKey == "" {
			return fmt.Errorf("profile %s has empty model_key", name)
		}
	}

	// Validate Default Exchange info
	if err := validateExchangeInfo("default_exchange", cfg.DefaultExchangeInfo); err != nil {
		return err
	}

	// Validate specific exchange overrides if present
	for exchange, info := range cfg.ExchangeInfo {
		if err := validateExchangeInfo(exchange, info); err != nil {
			return err
		}
	}

	return nil
}

func validateExchangeInfo(name string, info ExchangeInfo) error {
	// Validate timezone
	_, err := ParseTimezone(info.Timezone)
	if err != nil {
		return fmt.Errorf("exchange %s: %w", name, err)
	}

	// Validate time formats
	times := []struct {
		name  string
		value string
	}{
		{"premarket_start", info.PreMarketStart},
		{"market_open", info.MarketOpen},
		{"midday_start", info.MiddayStart},
		{"close_start", info.CloseStart},
		{"market_close", info.MarketClose},
		{"afterhours_end", info.AfterHoursEnd},
	}

	for _, t := range times {
		_, err := ParseTimeOfDay(t.value)
		if err != nil {
			return fmt.Errorf("exchange %s: invalid %s (%s): %w", name, t.name, t.value, err)
		}
	}

	// Validate weekdays
	if len(info.TradingWeekdays) == 0 {
		return fmt.Errorf("exchange %s: trading_weekdays cannot be empty", name)
	}
	_, err = ParseWeekdays(info.TradingWeekdays)
	if err != nil {
		return fmt.Errorf("exchange %s: %w", name, err)
	}

	return nil
}

func ParseTimeOfDay(s string) (time.Duration, error) {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, err
	}

	duration := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute
	return duration, nil
}

func ParseWeekdays(names []string) (map[time.Weekday]bool, error) {

	weekdayMap := map[string]time.Weekday{
		"Monday":    time.Monday,
		"Tuesday":   time.Tuesday,
		"Wednesday": time.Wednesday,
		"Thursday":  time.Thursday,
		"Friday":    time.Friday,
		"Saturday":  time.Saturday,
		"Sunday":    time.Sunday,
	}

	result := make(map[time.Weekday]bool)
	for _, name := range names {
		wd, ok := weekdayMap[name]
		if !ok {
			return nil, fmt.Errorf("invalid weekday name: %s", name)
		}
		result[wd] = true
	}

	return result, nil
}

func ParseTimezone(tz string) (*time.Location, error) {
	// time.LoadLocation uses IANA timezone database
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("failed to load timezone %s: %w", tz, err)
	}
	return loc, nil
}

// Note: BuildSessionResolver will be implemented after model.SessionResolver is created
// to avoid import cycles. This is a placeholder comment showing where it will go.
