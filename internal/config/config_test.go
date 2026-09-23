package config

import (
	"os"
	"path/filepath"
	"testing"
)

// Helper function to create a valid default exchange for tests
func getTestDefaultExchange() ExchangeInfo {
	return ExchangeInfo{
		Timezone:        "America/New_York",
		PreMarketStart:  "04:00",
		MarketOpen:      "09:30",
		MiddayStart:     "12:00",
		CloseStart:      "15:30",
		MarketClose:     "16:00",
		AfterHoursEnd:   "20:00",
		TradingWeekdays: []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"},
	}
}

func TestLoad_ValidConfig(t *testing.T) {
	// Use the actual config file from the project
	cfg, err := Load("../../config/aggregator.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	// Verify key fields loaded correctly
	if len(cfg.Kafka.Brokers) == 0 {
		t.Error("Expected brokers to be loaded")
	}
	if cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("Expected broker 'localhost:9092', got '%s'", cfg.Kafka.Brokers[0])
	}
	if cfg.Kafka.InputTopic != "raw-ticks" {
		t.Errorf("Expected input_topic 'raw-ticks', got '%s'", cfg.Kafka.InputTopic)
	}
	if cfg.Kafka.OutputTopic != "normalized-vectors" {
		t.Errorf("Expected output_topic 'normalized-vectors', got '%s'", cfg.Kafka.OutputTopic)
	}
	if cfg.Windows.FastWindowTicks != 60 {
		t.Errorf("Expected fast_window_ticks 60, got %f", cfg.Windows.FastWindowTicks)
	}
	if cfg.Windows.SlowWindowTicks != 700 {
		t.Errorf("Expected slow_window_ticks 700, got %f", cfg.Windows.SlowWindowTicks)
	}
	if cfg.DefaultExchangeInfo.Timezone == "" {
		t.Error("Expected default_exchange to be loaded")
	}
	// The data's timestamps are exchange-local wall-clock time labelled UTC, so the
	// sessions are written on that clock (see config/aggregator.yaml).
	if cfg.DefaultExchangeInfo.Timezone != "UTC" || cfg.DefaultExchangeInfo.MarketOpen != "09:00" ||
		cfg.DefaultExchangeInfo.MarketClose != "17:30" {
		t.Errorf("Expected European session hours on the data clock (UTC, 09:00-17:30), got %+v", cfg.DefaultExchangeInfo)
	}
	if len(cfg.Profiles) == 0 {
		t.Error("Expected profiles to be loaded")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("nonexistent_file_12345.yaml")
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestLoad_MalformedYAML(t *testing.T) {
	// Create temp file with invalid YAML
	tmpDir := t.TempDir()
	badFile := filepath.Join(tmpDir, "bad.yaml")

	err := os.WriteFile(badFile, []byte("this is not: valid: yaml: data: [[["), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err = Load(badFile)
	if err == nil {
		t.Error("Expected error for malformed YAML")
	}
}

func TestValidate_MissingKafkaBrokers(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers: []string{}, // Empty - should fail
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for missing brokers")
	}
}

func TestValidate_MissingKafkaTopics(t *testing.T) {
	tests := []struct {
		name   string
		config KafkaConfig
	}{
		{
			name: "missing input_topic",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				InputTopic:    "", // Missing
				OutputTopic:   "output",
				AlertTopic:    "alert",
				ConsumerGroup: "group",
			},
		},
		{
			name: "missing output_topic",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				InputTopic:    "input",
				OutputTopic:   "", // Missing
				AlertTopic:    "alert",
				ConsumerGroup: "group",
			},
		},
		{
			name: "missing alert_topic",
			config: KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				InputTopic:    "input",
				OutputTopic:   "output",
				AlertTopic:    "", // Missing
				ConsumerGroup: "group",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AggregatorConfig{
				Kafka: tt.config,
			}
			err := cfg.Validate()
			if err == nil {
				t.Error("Expected validation error")
			}
		})
	}
}

func TestValidate_InvalidWindows(t *testing.T) {
	tests := []struct {
		name      string
		fastTicks float64
		slowTicks float64
		wantError bool
	}{
		{"fast negative", -1, 1000, true},
		{"slow negative", 60, -1, true},
		{"fast zero", 0, 1000, true},
		{"slow zero", 60, 0, true},
		{"fast >= slow", 1000, 60, true},
		{"fast == slow", 100, 100, true},
		{"valid", 60, 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AggregatorConfig{
				Kafka: KafkaConfig{
					Brokers:       []string{"localhost:9092"},
					InputTopic:    "input",
					OutputTopic:   "output",
					AlertTopic:    "alert",
					ConsumerGroup: "group",
				},
				Windows: WindowConfig{
					FastWindowTicks: tt.fastTicks,
					SlowWindowTicks: tt.slowTicks,
				},
				CUSUM: CUSUMConfig{
					Slack:     0.5,
					Threshold: 5.0,
				},
				Silence: SilenceConfig{
					CheckIntervalSec: 5,
					GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				DefaultExchangeInfo: getTestDefaultExchange(),
			}

			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected validation error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestValidate_InvalidCUSUM(t *testing.T) {
	tests := []struct {
		name      string
		slack     float64
		threshold float64
		wantError bool
	}{
		{"negative slack", -0.1, 5.0, true},
		{"negative threshold", 0.5, -1.0, true},
		{"zero threshold", 0.5, 0, true},
		{"valid", 0.5, 5.0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AggregatorConfig{
				Kafka: KafkaConfig{
					Brokers:       []string{"localhost:9092"},
					InputTopic:    "input",
					OutputTopic:   "output",
					AlertTopic:    "alert",
					ConsumerGroup: "group",
				},
				Windows: WindowConfig{
					FastWindowTicks: 60,
					SlowWindowTicks: 1000,
				},
				CUSUM: CUSUMConfig{
					Slack:     tt.slack,
					Threshold: tt.threshold,
				},
				Silence: SilenceConfig{
					CheckIntervalSec: 5,
					GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				DefaultExchangeInfo: getTestDefaultExchange(),
			}

			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected validation error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestValidate_InvalidSilence(t *testing.T) {
	tests := []struct {
		name       string
		interval   int
		quantile   float64
		multiplier float64
		minObs     int64
		minThresh  float64
		wantError  bool
	}{
		{"negative interval", -1, 0.999, 1.0, 50, 1000, true},
		{"zero interval", 0, 0.999, 1.0, 50, 1000, true},
		{"quantile zero", 5, 0, 1.0, 50, 1000, true},
		{"quantile one", 5, 1, 1.0, 50, 1000, true},
		{"quantile above one", 5, 1.5, 1.0, 50, 1000, true},
		{"negative multiplier", 5, 0.999, -1.0, 50, 1000, true},
		{"zero multiplier", 5, 0.999, 0, 50, 1000, true},
		{"no minimum observations", 5, 0.999, 1.0, 0, 1000, true},
		{"negative floor", 5, 0.999, 1.0, 50, -1, true},
		{"valid", 5, 0.999, 1.0, 50, 1000, false},
		{"valid without floor", 5, 0.99, 2.0, 10, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &AggregatorConfig{
				Kafka: KafkaConfig{
					Brokers:       []string{"localhost:9092"},
					InputTopic:    "input",
					OutputTopic:   "output",
					AlertTopic:    "alert",
					ConsumerGroup: "group",
				},
				Windows: WindowConfig{
					FastWindowTicks: 60,
					SlowWindowTicks: 1000,
				},
				CUSUM: CUSUMConfig{
					Slack:     0.5,
					Threshold: 5.0,
				},
				Silence: SilenceConfig{
					CheckIntervalSec:      tt.interval,
					GapQuantile:           tt.quantile,
					GapQuantileMultiplier: tt.multiplier,
					MinObservations:       tt.minObs,
					MinThresholdMs:        tt.minThresh,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				DefaultExchangeInfo: getTestDefaultExchange(),
			}

			err := cfg.Validate()
			if tt.wantError && err == nil {
				t.Error("Expected validation error but got none")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Unexpected validation error: %v", err)
			}
		})
	}
}

func TestValidate_InvalidExchangeTimezone(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		DefaultExchangeInfo: ExchangeInfo{
			Timezone:        "Invalid/Timezone", // Bad timezone
			PreMarketStart:  "04:00",
			MarketOpen:      "09:30",
			MiddayStart:     "12:00",
			CloseStart:      "15:30",
			MarketClose:     "16:00",
			AfterHoursEnd:   "20:00",
			TradingWeekdays: []string{"Monday"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid timezone")
	}
}

func TestValidate_InvalidExchangeTime(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		DefaultExchangeInfo: ExchangeInfo{
			Timezone:        "America/New_York",
			PreMarketStart:  "25:00", // Invalid time
			MarketOpen:      "09:30",
			MiddayStart:     "12:00",
			CloseStart:      "15:30",
			MarketClose:     "16:00",
			AfterHoursEnd:   "20:00",
			TradingWeekdays: []string{"Monday"},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid time format")
	}
}

func TestValidate_EmptyProfiles(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles:            map[string]ClassProfile{}, // Empty - should fail
		DefaultExchangeInfo: getTestDefaultExchange(),
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for empty profiles")
	}
}

func TestValidate_ProfileWithEmptyModelKey(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: ""}, // Empty model_key
		},
		DefaultExchangeInfo: getTestDefaultExchange(),
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for profile with empty model_key")
	}
}

func TestValidate_EmptyDefaultExchange(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		DefaultExchangeInfo: ExchangeInfo{}, // Empty - should fail
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for empty default exchange")
	}
}

func TestValidate_InvalidWeekday(t *testing.T) {
	cfg := &AggregatorConfig{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			InputTopic:    "input",
			OutputTopic:   "output",
			AlertTopic:    "alert",
			ConsumerGroup: "group",
		},
		Windows: WindowConfig{
			FastWindowTicks: 60,
			SlowWindowTicks: 1000,
		},
		CUSUM: CUSUMConfig{
			Slack:     0.5,
			Threshold: 5.0,
		},
		Silence: SilenceConfig{
			CheckIntervalSec: 5,
			GapQuantile:      0.999, GapQuantileMultiplier: 1.0, MinObservations: 50,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		DefaultExchangeInfo: ExchangeInfo{
			Timezone:        "America/New_York",
			PreMarketStart:  "04:00",
			MarketOpen:      "09:30",
			MiddayStart:     "12:00",
			CloseStart:      "15:30",
			MarketClose:     "16:00",
			AfterHoursEnd:   "20:00",
			TradingWeekdays: []string{"InvalidDay"}, // Bad weekday
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid weekday")
	}
}

func TestLoad_AlertsAndValidationSections(t *testing.T) {
	cfg, err := Load("../../config/aggregator.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	if !cfg.Validation.Enabled || cfg.Validation.TimestampToleranceMs != 1000 {
		t.Errorf("unexpected validation config: %+v", cfg.Validation)
	}
	if cfg.Alerts.SilenceLog == "" || cfg.Alerts.ValidationLog == "" {
		t.Errorf("expected both evaluation logs to be configured: %+v", cfg.Alerts)
	}
	if cfg.Alerts.KafkaSilenceEnabled() {
		t.Error("the shipped config sends silence alerts to the log only")
	}
}

func TestAlertsConfig_KafkaSilenceDefaultsToEnabled(t *testing.T) {
	if !(AlertsConfig{}).KafkaSilenceEnabled() {
		t.Error("omitting kafka_silence_alerts must keep publishing to Kafka")
	}

	off := false
	if (AlertsConfig{KafkaSilenceAlerts: &off}).KafkaSilenceEnabled() {
		t.Error("kafka_silence_alerts: false must disable Kafka publishing")
	}
}

func TestValidate_NegativeTimestampTolerance(t *testing.T) {
	cfg, err := Load("../../config/aggregator.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	cfg.Validation.TimestampToleranceMs = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative timestamp_tolerance_ms should be rejected")
	}
}

func TestLoad_SilenceAndPriceWarmupSettings(t *testing.T) {
	cfg, err := Load("../../config/aggregator.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}
	if cfg.Silence.GapQuantile != 0.999 || cfg.Silence.GapQuantileMultiplier != 1.0 ||
		cfg.Silence.MinObservations != 50 || cfg.Silence.MinThresholdMs != 1000 {
		t.Errorf("unexpected silence config: %+v", cfg.Silence)
	}
	if cfg.Windows.MinPriceObservations != 20 {
		t.Errorf("min_price_observations: got %d, want 20", cfg.Windows.MinPriceObservations)
	}
}

func TestLimitsDefaultsAndOverrides(t *testing.T) {
	var cfg AggregatorConfig
	l := cfg.Limits()
	if l.WinsorZ != 10 || !l.ResetCusumDaily {
		t.Errorf("defaults: want winsor 10 and daily reset, got %v / %v", l.WinsorZ, l.ResetCusumDaily)
	}
	off := false
	cfg.Windows.WinsorZ = -1
	cfg.CUSUM.ResetDaily = &off
	l = cfg.Limits()
	if l.WinsorZ != 0 || l.ResetCusumDaily {
		t.Errorf("disabled: want winsor 0 and no reset, got %v / %v", l.WinsorZ, l.ResetCusumDaily)
	}
	cfg.Windows.WinsorZ = 7
	if got := cfg.Limits().WinsorZ; got != 7 {
		t.Errorf("explicit winsor_z: got %v", got)
	}
}
