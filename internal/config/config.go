// Package config includes the Configuration for feed-handler
// inlucdes all thresholds, kafka configuration, instrument profile configuration
// consurems the infromation through yaml file
package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type AggregatorConfig struct {
	Kafka    KafkaConfig             `yaml:"kafka"`
	Windows  WindowConfig            `yaml:"windows"`
	CUSUM    CUSUMConfig             `yaml:"cusum"`
	Silence  SilenceConfig           `yaml:"silence"`
	Profiles map[string]ClassProfile `yaml:"profiles"`
}

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	InputTopic    string   `yaml:"input_topic"`
	OutputTopic   string   `yaml:"output_topic"`
	AlertTopic    string   `yaml:"alert_topic"`
	ConsumerGroup string   `yaml:"consumer_group"`
}

type WindowConfig struct {
	FastWindowSec float64 `yaml:"fast_window_sec"`
	SlowWindowSec float64 `yaml:"slow_window_sec"`
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
