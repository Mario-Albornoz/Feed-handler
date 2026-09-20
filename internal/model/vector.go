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

}
