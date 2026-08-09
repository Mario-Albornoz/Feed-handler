// Package processing contains logic for processing ticks from the kafka consumer
package processing

import (
	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type FeedProcessor struct {
	config             *config.AggregatorConfig
	sessionResolver    *model.SessionResolver
	instrumentRegistry *model.InstrumentRegistry
}

func NewFeedProcessor(aggregatorConfig config.AggregatorConfig, resolver *model.SessionResolver, instrumentRegistry *model.InstrumentRegistry) *FeedProcessor {
	return &FeedProcessor{
		config:             &aggregatorConfig,
		sessionResolver:    resolver,
		instrumentRegistry: instrumentRegistry,
	}
}

func (fp *FeedProcessor) process() {

}
