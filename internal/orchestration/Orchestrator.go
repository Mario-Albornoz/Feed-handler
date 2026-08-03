// Package orchestration holds all structs related to routing the receiving tick to the correct Session bucket
package orchestration

import (
	"github.com/mario-albornoz/feed-handler-aggregator/internal/config"
	"github.com/mario-albornoz/feed-handler-aggregator/internal/model"
)

type Orchestrator struct {
	config             *config.AggregatorConfig
	sessionResolver    *model.SessionResolver
	instrumentRegistry map[model.InstrumentKey]*model.InstrumentState
}

func NewOrchestrator(cfg *config.AggregatorConfig, resolver *model.SessionResolver) *Orchestrator {
	return &Orchestrator{
		config:             cfg,
		sessionResolver:    resolver,
		instrumentRegistry: make(map[model.InstrumentKey]*model.InstrumentState),
	}
}
