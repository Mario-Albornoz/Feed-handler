// Package processing contains logic for processing ticks from the kafka consumer
package processing

import (
	"context"
	"log"

	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type VectorEmitter interface {
	WriteVector(ctx context.Context, vector *model.NormalizedVector) error
}

type FeedProcessor struct {
	pipeline []TickProcessor
}

func NewFeedProcessor(
	aggregatorConfig config.AggregatorConfig,
	resolver *model.SessionResolver,
	instrumentRegistry *model.InstrumentRegistry,
	producer VectorEmitter,
) *FeedProcessor {
	pipeline := []TickProcessor{
		&EquityFilterProcessor{},
		&SessionResolverProcessor{resolver: resolver},
		&InstrumentLookupProcessor{registry: instrumentRegistry},
		&MetricsCalculatorProcessor{},
		&StatsUpdaterProcessor{},
		&FallbackSelectorProcessor{},
		&ZScoreCalculatorProcessor{},
		&CusumExtractorProcessor{},
		&FlagCalculatorProcessor{},
		&VectorBuilderProcessor{config: &aggregatorConfig},
		&VectorEmitterProcessor{producer: producer},
		&StateUpdaterProcessor{},
	}

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
