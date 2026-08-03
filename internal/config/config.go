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
	Kafka        KafkaConfig             `yaml:"kafka"`
	Windows      WindowConfig            `yaml:"windows"`
	CUSUM        CUSUMConfig             `yaml:"cusum"`
	Silence      SilenceConfig           `yaml:"silence"`
	Profiles     map[string]ClassProfile `yaml:"profiles"`
	ExchangeInfo map[string]ExchangeInfo `yaml:"exchanges"`
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
		return nil, err
	}
	var cfg AggregatorConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
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
