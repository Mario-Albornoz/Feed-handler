package model

import (
	"encoding/json"
	"testing"
	"time"
)

// simulatorMessage is what the simulator publishes for a trade: json.Marshal of its own
// tick struct. Keep it literal: it is the contract between the two programs.
const simulatorMessage = `{"ID":"SAP.ETR","Exchange":"ETR","SecType":"E","ISIN":"","Bid":0,"Ask":0,` +
	`"TotalVolume":1000,"Last":123.45,"Seq":987654,"TradingTime":"2021-11-10T10:00:00.512Z",` +
	`"Date":"2021-11-10T00:00:00Z","Time":"2021-11-10T10:00:00Z"}`

func TestWireFormat_PriceReachesTheHandler(t *testing.T) {
	var tick RawTick
	if err := json.Unmarshal([]byte(simulatorMessage), &tick); err != nil {
		t.Fatal(err)
	}
	if tick.LastTradedPrice != 123.45 {
		t.Fatalf("the last price must survive the wire, got %v (a name mismatch decodes silently to 0)", tick.LastTradedPrice)
	}
	if tick.Seq != 987654 {
		t.Errorf("the message sequence number must survive the wire, got %d", tick.Seq)
	}
	if tick.TotalVolume != 1000 || tick.ID != "SAP.ETR" || tick.SecType != "E" {
		t.Errorf("unexpected decoding: %+v", tick)
	}
}

func TestFeatureTimeIsTheWholeSecondUpdateTime(t *testing.T) {
	var tick RawTick
	json.Unmarshal([]byte(simulatorMessage), &tick)

	want := time.Date(2021, 11, 10, 10, 0, 0, 0, time.UTC)
	if got := tick.FeatureTime(); !got.Equal(want) {
		t.Errorf("FeatureTime: got %v, want the update time %v", got, want)
	}
	if !tick.TradingTime.After(want) {
		t.Error("test setup: TradingTime should carry milliseconds")
	}

	noUpdateTime := RawTick{TradingTime: tick.TradingTime}
	if !noUpdateTime.FeatureTime().Equal(tick.TradingTime) {
		t.Error("without an update time FeatureTime falls back to TradingTime")
	}
}
