// Package config includes the Configuration for feed-handler
// inlucdes all thresholds, kafka configuration, instrument profile configuration
// consurems the infromation through yaml file
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
	"gopkg.in/yaml.v3"
)

type AggregatorConfig struct {
	Kafka               KafkaConfig             `yaml:"kafka"`
	Windows             WindowConfig            `yaml:"windows"`
	CUSUM               CUSUMConfig             `yaml:"cusum"`
	Silence             SilenceConfig           `yaml:"silence"`
	Validation          ValidationConfig        `yaml:"validation"`
	Alerts              AlertsConfig            `yaml:"alerts"`
	Stats               StatsConfig             `yaml:"stats"`
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
	// MinPriceObservations is how many trade-to-trade price steps an instrument needs
	// before its price statistics count as warm (default 20). Trades are rare.
	MinPriceObservations int64 `yaml:"min_price_observations"`
	// TimingResolutionMs and PriceResolutionBps are the resolutions of the measurements
	// (the feature clock and the price grid). A standard deviation is never taken below
	// resolution/sqrt(12), the rounding noise. 0 keeps the defaults (1000 ms, 1 bp).
	TimingResolutionMs float64 `yaml:"timing_resolution_ms"`
	PriceResolutionBps float64 `yaml:"price_resolution_bps"`
}

type CUSUMConfig struct {
	Slack     float64 `yaml:"slack"`
	Threshold float64 `yaml:"threshold"`
	// ZClip bounds the z-score one observation adds to a CUSUM (0 keeps the default, 10).
	ZClip float64 `yaml:"z_clip"`
}

// SilenceConfig configures silence detection: an instrument is silent when it has been
// quiet for longer than gap_quantile of its own past gaps (times gap_quantile_multiplier).
type SilenceConfig struct {
	CheckIntervalSec int `yaml:"check_interval_sec"` // wall-clock cadence of the periodic scan
	// GapQuantile is the quantile of an instrument's own gaps used as the threshold.
	GapQuantile float64 `yaml:"gap_quantile"`
	// GapQuantileMultiplier scales the quantile; alerts are logged at this value.
	GapQuantileMultiplier float64 `yaml:"gap_quantile_multiplier"`
	// MinObservations is how many gaps an instrument needs before it can alert.
	MinObservations int64 `yaml:"min_observations"`
	// MinThresholdMs is a floor: the resolution of the clock (whole seconds).
	MinThresholdMs float64 `yaml:"min_threshold_ms"`
}

// ValidationConfig controls the feed-integrity validator (malformed ISIN, timestamp
// inversion). When disabled, ticks flow to the pipeline unchecked.
type ValidationConfig struct {
	Enabled bool `yaml:"enabled"`
	// TimestampToleranceMs is how far behind an instrument's last accepted tick a
	// timestamp may fall before it is an inversion. Normal update times step
	// backwards by up to a second (see validation.New), so use at least 1000.
	TimestampToleranceMs int64 `yaml:"timestamp_tolerance_ms"`
}

// AlertsConfig says where the detectors write their alerts. The CSV logs are the
// input of the thesis evaluation; leave a path empty to disable that log.
type AlertsConfig struct {
	// SilenceLog is the CSV file for silence alerts.
	SilenceLog string `yaml:"silence_log"`
	// ValidationLog is the CSV file for validator alerts.
	ValidationLog string `yaml:"validation_log"`
	// KafkaSilenceAlerts also publishes silence alerts to kafka.alert_topic.
	// Defaults to true when omitted.
	KafkaSilenceAlerts *bool `yaml:"kafka_silence_alerts"`
}

// Limits returns the z-score limits, with the defaults for values left at 0.
func (cfg *AggregatorConfig) Limits() stats.Limits {
	return stats.NewLimits(
		orDefault(cfg.Windows.TimingResolutionMs, stats.DefaultTimingResolutionMs),
		orDefault(cfg.Windows.PriceResolutionBps, stats.DefaultPriceResolutionBps),
		orDefault(cfg.CUSUM.ZClip, stats.DefaultCusumZClip),
	)
}

func orDefault(v, def float64) float64 {
	if v > 0 {
		return v
	}
	return def
}

// KafkaSilenceEnabled reports whether silence alerts are published to Kafka.
func (a AlertsConfig) KafkaSilenceEnabled() bool {
	return a.KafkaSilenceAlerts == nil || *a.KafkaSilenceAlerts
}

type StatsConfig struct {
	ReportIntervalSec int `yaml:"report_interval_sec"` // How often to report statistics (default: 5s)
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
	if cfg.CUSUM.ZClip < 0 {
		return fmt.Errorf("cusum z_clip must be non-negative, got %f", cfg.CUSUM.ZClip)
	}
	if cfg.Windows.TimingResolutionMs < 0 || cfg.Windows.PriceResolutionBps < 0 {
		return fmt.Errorf("timing_resolution_ms and price_resolution_bps must be non-negative, got %f and %f",
			cfg.Windows.TimingResolutionMs, cfg.Windows.PriceResolutionBps)
	}

	// Validate Silence config
	if cfg.Silence.CheckIntervalSec <= 0 {
		return fmt.Errorf("silence check_interval_sec must be positive, got %d", cfg.Silence.CheckIntervalSec)
	}
	if cfg.Silence.GapQuantile <= 0 || cfg.Silence.GapQuantile >= 1 {
		return fmt.Errorf("silence gap_quantile must be in (0, 1), got %f", cfg.Silence.GapQuantile)
	}
	if cfg.Silence.GapQuantileMultiplier <= 0 {
		return fmt.Errorf("silence gap_quantile_multiplier must be positive, got %f", cfg.Silence.GapQuantileMultiplier)
	}
	if cfg.Silence.MinObservations < 1 {
		return fmt.Errorf("silence min_observations must be at least 1, got %d", cfg.Silence.MinObservations)
	}
	if cfg.Silence.MinThresholdMs < 0 {
		return fmt.Errorf("silence min_threshold_ms must be non-negative, got %f", cfg.Silence.MinThresholdMs)
	}
	if cfg.Windows.MinPriceObservations < 0 {
		return fmt.Errorf("min_price_observations must be non-negative, got %d", cfg.Windows.MinPriceObservations)
	}

	// Validate Validation config
	if cfg.Validation.TimestampToleranceMs < 0 {
		return fmt.Errorf("validation timestamp_tolerance_ms must be non-negative, got %d", cfg.Validation.TimestampToleranceMs)
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
