package config

import (
	"os"
	"path/filepath"
	"testing"
)

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
	if cfg.Windows.SlowWindowTicks != 14400 {
		t.Errorf("Expected slow_window_ticks 14400, got %f", cfg.Windows.SlowWindowTicks)
	}
	if len(cfg.ExchangeInfo) == 0 {
		t.Error("Expected exchanges to be loaded")
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
					GapMultiplier:    5.0,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				ExchangeInfo: map[string]ExchangeInfo{
					"NYSE": {
						Timezone:        "America/New_York",
						PreMarketStart:  "04:00",
						MarketOpen:      "09:30",
						MiddayStart:     "12:00",
						CloseStart:      "15:30",
						MarketClose:     "16:00",
						AfterHoursEnd:   "20:00",
						TradingWeekdays: []string{"Monday", "Tuesday"},
					},
				},
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
					GapMultiplier:    5.0,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				ExchangeInfo: map[string]ExchangeInfo{
					"NYSE": {
						Timezone:        "America/New_York",
						PreMarketStart:  "04:00",
						MarketOpen:      "09:30",
						MiddayStart:     "12:00",
						CloseStart:      "15:30",
						MarketClose:     "16:00",
						AfterHoursEnd:   "20:00",
						TradingWeekdays: []string{"Monday"},
					},
				},
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
		name             string
		checkIntervalSec int
		gapMultiplier    float64
		wantError        bool
	}{
		{"negative interval", -1, 5.0, true},
		{"zero interval", 0, 5.0, true},
		{"negative multiplier", 5, -1.0, true},
		{"zero multiplier", 5, 0, true},
		{"valid", 5, 5.0, false},
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
					CheckIntervalSec: tt.checkIntervalSec,
					GapMultiplier:    tt.gapMultiplier,
				},
				Profiles: map[string]ClassProfile{
					"equity": {ModelKey: "equity"},
				},
				ExchangeInfo: map[string]ExchangeInfo{
					"NYSE": {
						Timezone:        "America/New_York",
						PreMarketStart:  "04:00",
						MarketOpen:      "09:30",
						MiddayStart:     "12:00",
						CloseStart:      "15:30",
						MarketClose:     "16:00",
						AfterHoursEnd:   "20:00",
						TradingWeekdays: []string{"Monday"},
					},
				},
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		ExchangeInfo: map[string]ExchangeInfo{
			"NYSE": {
				Timezone:        "Invalid/Timezone", // Bad timezone
				PreMarketStart:  "04:00",
				MarketOpen:      "09:30",
				MiddayStart:     "12:00",
				CloseStart:      "15:30",
				MarketClose:     "16:00",
				AfterHoursEnd:   "20:00",
				TradingWeekdays: []string{"Monday"},
			},
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		ExchangeInfo: map[string]ExchangeInfo{
			"NYSE": {
				Timezone:        "America/New_York",
				PreMarketStart:  "25:00", // Invalid time
				MarketOpen:      "09:30",
				MiddayStart:     "12:00",
				CloseStart:      "15:30",
				MarketClose:     "16:00",
				AfterHoursEnd:   "20:00",
				TradingWeekdays: []string{"Monday"},
			},
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{}, // Empty - should fail
		ExchangeInfo: map[string]ExchangeInfo{
			"NYSE": {
				Timezone:        "America/New_York",
				PreMarketStart:  "04:00",
				MarketOpen:      "09:30",
				MiddayStart:     "12:00",
				CloseStart:      "15:30",
				MarketClose:     "16:00",
				AfterHoursEnd:   "20:00",
				TradingWeekdays: []string{"Monday"},
			},
		},
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: ""}, // Empty model_key
		},
		ExchangeInfo: map[string]ExchangeInfo{
			"NYSE": {
				Timezone:        "America/New_York",
				PreMarketStart:  "04:00",
				MarketOpen:      "09:30",
				MiddayStart:     "12:00",
				CloseStart:      "15:30",
				MarketClose:     "16:00",
				AfterHoursEnd:   "20:00",
				TradingWeekdays: []string{"Monday"},
			},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for profile with empty model_key")
	}
}

func TestValidate_EmptyExchanges(t *testing.T) {
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		ExchangeInfo: map[string]ExchangeInfo{}, // Empty - should fail
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for empty exchanges")
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
			GapMultiplier:    5.0,
		},
		Profiles: map[string]ClassProfile{
			"equity": {ModelKey: "equity"},
		},
		ExchangeInfo: map[string]ExchangeInfo{
			"NYSE": {
				Timezone:        "America/New_York",
				PreMarketStart:  "04:00",
				MarketOpen:      "09:30",
				MiddayStart:     "12:00",
				CloseStart:      "15:30",
				MarketClose:     "16:00",
				AfterHoursEnd:   "20:00",
				TradingWeekdays: []string{"InvalidDay"}, // Bad weekday
			},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid weekday")
	}
}
