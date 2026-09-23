package model

import "time"

type NormalizedVector struct {
	Exchange   string    `json:"exchange"`
	Instrument string    `json:"instrument"`
	Class      string    `json:"class"`
	Timestamp  time.Time `json:"timestamp"`
	ModelKey   string    `json:"model_key"`

	// Seq identifies the message this vector was computed from (0 if the producer sends
	// none). It goes through to the detector's scores.
	Seq uint64 `json:"seq"`

	ZIntertickFast float64 `json:"z_intertick_fast"`
	ZPriceStepFast float64 `json:"z_price_step_fast"`

	ZIntertickSlow float64 `json:"z_intertick_slow"`
	ZPriceStepSlow float64 `json:"z_price_step_slow"`

	CusumIntertick float64 `json:"cusum_intertick"`
	CusumPriceStep float64 `json:"cusum_price_step"`

	GapFlag int `json:"gap_flag"` // 1 if interval > 5x rolling mean

	// HasTrade is 1 if the message carried a trade (a last traded price). Most messages
	// are quote updates; on those the price z-scores are 0 and the price CUSUM is the
	// value carried over from the last trade.
	HasTrade int `json:"has_trade"`

	WarmupFlag          int `json:"warmup_flag"`           // 1 if < 50 observations total
	SessionFallbackFlag int `json:"session_fallback_flag"` // 1 if used fallback instead of session stats

	// The raw measurements the z-scores are computed from, before any normalization, so
	// an ablation can score the same messages on un-normalized features.
	// IntertickMs is the time since the instrument's previous message on the feature clock
	// (whole seconds); HasIntertick is 0 for the first message of an instrument or of a
	// day, whose IntertickMs is a placeholder 0. PriceStep is the absolute change of the
	// traded price since the instrument's previous trade (0 unless HasPriceStep), and
	// RefPrice that previous traded price (0 before the first trade), so the step can be
	// expressed relative to the price level.
	IntertickMs  float64 `json:"intertick_ms"`
	HasIntertick int     `json:"has_intertick"`
	PriceStep    float64 `json:"price_step"`
	HasPriceStep int     `json:"has_price_step"`
	RefPrice     float64 `json:"ref_price"`
}
