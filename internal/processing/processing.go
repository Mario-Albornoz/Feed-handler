// Package processing contains logic for processing ticks from the kafka consumer
package processing

import (
	"context"
	"log"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/validation"
)

type VectorEmitter interface {
	WriteVector(ctx context.Context, vector *model.NormalizedVector) error
}

type FeedProcessor struct {
	pipeline []TickProcessor
}

// Option customises the pipeline built by NewFeedProcessor.
type Option func(*pipelineOptions)

type pipelineOptions struct {
	validator *validation.Validator
	observer  SilenceObserver
}

// WithValidator quarantines malformed ticks (bad ISIN, timestamp inversion) ahead of
// the feature pipeline.
func WithValidator(v *validation.Validator) Option {
	return func(o *pipelineOptions) { o.validator = v }
}

// WithSilenceObserver reports every accepted tick to the silence detector.
func WithSilenceObserver(observer SilenceObserver) Option {
	return func(o *pipelineOptions) { o.observer = observer }
}

func NewFeedProcessor(
	aggregatorConfig config.AggregatorConfig,
	resolver *model.SessionResolver,
	instrumentRegistry *model.InstrumentRegistry,
	producer VectorEmitter,
	opts ...Option,
) *FeedProcessor {
	var options pipelineOptions
	for _, opt := range opts {
		opt(&options)
	}

	pipeline := []TickProcessor{
		&EquityFilterProcessor{},
		&InstrumentLookupProcessor{registry: instrumentRegistry},
	}
	// Both run before any instrument state is updated: the validator so a rejected
	// tick leaves no trace, the observer so it sees the previous tick's time.
	if options.validator != nil {
		pipeline = append(pipeline, &ValidatorProcessor{validator: options.validator, observer: options.observer})
	}
	if options.observer != nil {
		pipeline = append(pipeline, &SilenceObserverProcessor{observer: options.observer})
	}
	pipeline = append(pipeline, []TickProcessor{
		&SessionResolverProcessor{resolver: resolver},
		&MetricsCalculatorProcessor{},
		&FallbackSelectorProcessor{},
		&ZScoreCalculatorProcessor{},
		&StatsUpdaterProcessor{},
		&CusumExtractorProcessor{},
		&FlagCalculatorProcessor{},
		&VectorBuilderProcessor{config: &aggregatorConfig},
		&VectorEmitterProcessor{producer: producer},
		&StateUpdaterProcessor{},
	}...)

	return &FeedProcessor{
		pipeline: pipeline,
	}
}

// ProcessRawTicks processes a single raw tick through the pipeline
func (fp *FeedProcessor) ProcessRawTicks(ctx context.Context, rawTick *model.RawTick) error {
	state := &ProcessingState{
		Tick: rawTick,
	}

	for _, processor := range fp.pipeline {
		if err := processor.Process(ctx, state); err != nil {
			// ErrSkipTick is not an error, just means we're done with this tick
			if err == ErrSkipTick {
				return nil
			}
			log.Printf("Pipeline error at %T: %v", processor, err)
			return err
		}
	}

	return nil
}

// AddProcessor allows adding a custom processor to the pipeline
func (fp *FeedProcessor) AddProcessor(processor TickProcessor) {
	fp.pipeline = append(fp.pipeline, processor)
}

// InsertProcessor inserts a processor at a specific position in the pipeline
func (fp *FeedProcessor) InsertProcessor(index int, processor TickProcessor) {
	fp.pipeline = append(fp.pipeline[:index], append([]TickProcessor{processor}, fp.pipeline[index:]...)...)
}
