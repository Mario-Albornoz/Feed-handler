package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/kafka"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/processing"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/silence"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/stats"
)

// System encapsulates all components of the aggregator
type System struct {
	config    *config.AggregatorConfig
	resolver  *model.SessionResolver
	registry  *model.InstrumentRegistry
	producer  *kafka.Producer
	processor *processing.FeedProcessor
	consumer  *kafka.FeedConsumer
	detector  *silence.Detector
	tracker   *stats.ThroughputTracker
}

func (s *System) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	consumerDone := s.startConsumer(ctx)
	detectorDone := s.startDetector(ctx)
	statsDone := s.startStatsReporter(ctx)

	log.Println("System fully operational. Press Ctrl+C to shutdown gracefully.")

	return s.waitForShutdown(ctx, cancel, consumerDone, detectorDone, statsDone)
}

func (s *System) Shutdown() {
	log.Println("Shutting down system...")

	if s.consumer != nil {
		log.Println("Closing Kafka consumer...")
		if err := s.consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}

	if s.producer != nil {
		log.Println("Closing Kafka producer...")
		if err := s.producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}

	if s.registry != nil {
		s.saveRegistry()
	}

	log.Println("Shutdown complete")
}

func (s *System) startConsumer(ctx context.Context) chan error {
	done := make(chan error, 1)
	go func() {
		done <- s.consumer.StartReadMessageLoop(ctx)
	}()
	log.Println("Kafka consumer started, processing ticks...")
	return done
}

func (s *System) startDetector(ctx context.Context) chan error {
	done := make(chan error, 1)
	go func() {
		done <- s.detector.Run(ctx)
	}()
	log.Println("Silence detector started")
	return done
}

func (s *System) startStatsReporter(ctx context.Context) chan error {
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(s.tracker.GetReportInterval())
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				// Final report before shutdown
				s.updateInstrumentStats()
				s.tracker.Report()
				done <- ctx.Err()
				return
			case <-ticker.C:
				s.updateInstrumentStats()
				s.tracker.Report()
			}
		}
	}()
	log.Printf("Statistics reporter started (interval: %s)", s.tracker.GetReportInterval())
	return done
}

func (s *System) updateInstrumentStats() {
	// Get instrument statistics
	allInstruments := s.registry.All()
	total := uint64(len(allInstruments))
	warm := uint64(0)
	
	for _, state := range allInstruments {
		if state.AllSessionStats.IsWarm() {
			warm++
		}
	}
	
	// Get consumer metrics
	ticksConsumed, consumerErrors := s.consumer.GetMetrics()
	
	// Get producer metrics
	vectorsPublished, alertsPublished, vectorErrors, alertErrors := s.producer.GetMetrics()
	
	// Update tracker with current values
	s.tracker.UpdateMetrics(
		ticksConsumed,
		ticksConsumed-consumerErrors, // Processed = consumed - failed to parse
		vectorsPublished,
		alertsPublished,
		consumerErrors,
		vectorErrors+alertErrors,
	)
	s.tracker.UpdateInstrumentStats(total, warm)
}

func (s *System) waitForShutdown(ctx context.Context, cancel context.CancelFunc, consumerDone, detectorDone, statsDone chan error) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var shutdownErr error

	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v, initiating graceful shutdown...", sig)
	case err := <-consumerDone:
		log.Printf("Consumer stopped: %v", err)
		shutdownErr = err
	case err := <-detectorDone:
		log.Printf("Silence detector stopped: %v", err)
		shutdownErr = err
	case err := <-statsDone:
		log.Printf("Statistics reporter stopped: %v", err)
		shutdownErr = err
	}

	cancel()

	s.waitForGoroutines(consumerDone, detectorDone, statsDone, 10*time.Second)

	return shutdownErr
}

func (s *System) waitForGoroutines(consumerDone, detectorDone, statsDone chan error, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	consumerStopped := false
	detectorStopped := false
	statsStopped := false

	for !consumerStopped || !detectorStopped || !statsStopped {
		select {
		case <-consumerDone:
			if !consumerStopped {
				consumerStopped = true
				log.Println("Consumer goroutine stopped")
			}
		case <-detectorDone:
			if !detectorStopped {
				detectorStopped = true
				log.Println("Silence detector goroutine stopped")
			}
		case <-statsDone:
			if !statsStopped {
				statsStopped = true
				log.Println("Statistics reporter goroutine stopped")
			}
		case <-timer.C:
			log.Println("Warning: Shutdown timeout reached, forcing exit")
			return
		}
	}
}

func (s *System) saveRegistry() {
	registryPath := getRegistryPath()

	log.Printf("Saving registry to %s...", registryPath)

	if err := os.MkdirAll("data", 0755); err != nil {
		log.Printf("Warning: Failed to create data directory: %v", err)
	}

	if err := s.registry.Save(registryPath); err != nil {
		log.Printf("Error saving registry: %v", err)
	} else {
		count := len(s.registry.All())
		log.Printf("Registry saved successfully (%d instruments)", count)
	}
}

type SystemBuilder struct {
	configPath string
	config     *config.AggregatorConfig
	resolver   *model.SessionResolver
	registry   *model.InstrumentRegistry
	producer   *kafka.Producer
	processor  *processing.FeedProcessor
	consumer   *kafka.FeedConsumer
	detector   *silence.Detector
	tracker    *stats.ThroughputTracker
	err        error
}

func NewSystemBuilder() *SystemBuilder {
	return &SystemBuilder{}
}

func (b *SystemBuilder) WithConfigPath(path string) *SystemBuilder {
	if b.err != nil {
		return b
	}
	b.configPath = path
	return b
}

func (b *SystemBuilder) WithConfig() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Printf("Loading configuration from %s...", b.configPath)
	cfg, err := config.Load(b.configPath)
	if err != nil {
		b.err = fmt.Errorf("failed to load config: %w", err)
		return b
	}

	b.config = cfg
	logConfiguration(cfg)
	return b
}

func (b *SystemBuilder) WithSessionResolver() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Building session resolver...")
	resolver, err := buildSessionResolver(b.config)
	if err != nil {
		b.err = fmt.Errorf("failed to build session resolver: %w", err)
		return b
	}

	b.resolver = resolver
	log.Printf("Session resolver initialized with %d exchanges", len(b.config.ExchangeInfo))
	return b
}

func (b *SystemBuilder) WithRegistry() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Initializing instrument registry...")

	b.registry = model.NewInstrumentRegistry(
		b.config.Windows.FastWindowTicks,
		b.config.Windows.SlowWindowTicks,
		b.config.CUSUM.Slack,
	)

	// Attempt to load existing registry
	registryPath := getRegistryPath()
	if err := b.registry.Load(registryPath); err != nil {
		log.Printf("No existing registry found (or load failed): %v", err)
		log.Println("Starting with empty registry (all instruments will be cold)")
	} else {
		count := len(b.registry.All())
		log.Printf("Registry loaded successfully from %s (%d instruments)", registryPath, count)
	}

	return b
}

func (b *SystemBuilder) WithTopics() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Ensuring Kafka topics exist...")

	topics := []kafka.TopicConfig{
		{
			Name:              b.config.Kafka.InputTopic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
		{
			Name:              b.config.Kafka.OutputTopic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
		{
			Name:              b.config.Kafka.AlertTopic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	}

	if err := kafka.EnsureTopicsExist(b.config.Kafka.Brokers[0], topics); err != nil {
		b.err = fmt.Errorf("failed to ensure topics exist: %w", err)
		return b
	}

	log.Println("Kafka topics verified")
	return b
}

func (b *SystemBuilder) WithProducer() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Creating Kafka producer...")

	b.producer = kafka.NewProducer(
		b.config.Kafka.OutputTopic,
		b.config.Kafka.AlertTopic,
		b.config.Kafka.Brokers[0], // TODO: Support multiple brokers
	)

	log.Printf("Kafka producer initialized (vectors → %s, alerts → %s)",
		b.config.Kafka.OutputTopic, b.config.Kafka.AlertTopic)

	return b
}

func (b *SystemBuilder) WithProcessor() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Creating feed processor...")
	b.processor = processing.NewFeedProcessor(*b.config, b.resolver, b.registry, b.producer)
	return b
}

func (b *SystemBuilder) WithConsumer() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Creating Kafka consumer...")
	b.consumer = kafka.NewFeedConsumer(b.config.Kafka, b.processor)
	log.Printf("Kafka consumer initialized (reading from %s)", b.config.Kafka.InputTopic)
	return b
}

func (b *SystemBuilder) WithDetector() *SystemBuilder {
	if b.err != nil {
		return b
	}

	log.Println("Creating silence detector...")

	b.detector = silence.NewDetector(
		b.registry,
		b.resolver,
		b.producer,
		b.config.Silence.GapMultiplier,
		time.Duration(b.config.Silence.CheckIntervalSec)*time.Second,
	)

	log.Println("Silence detector created")
	return b
}

func (b *SystemBuilder) WithThroughputTracker() *SystemBuilder {
	if b.err != nil {
		return b
	}

	reportInterval := 5 * time.Second // default
	if b.config.Stats.ReportIntervalSec > 0 {
		reportInterval = time.Duration(b.config.Stats.ReportIntervalSec) * time.Second
	}

	log.Printf("Creating throughput tracker (report interval: %s)...", reportInterval)
	b.tracker = stats.NewThroughputTracker(reportInterval)

	return b
}

func (b *SystemBuilder) Build() (*System, error) {
	if b.err != nil {
		return nil, b.err
	}

	return &System{
		config:    b.config,
		resolver:  b.resolver,
		registry:  b.registry,
		producer:  b.producer,
		processor: b.processor,
		consumer:  b.consumer,
		detector:  b.detector,
		tracker:   b.tracker,
	}, nil
}
