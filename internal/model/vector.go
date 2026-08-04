package model

import "time"

type NormalizedVector struct {
	Exchange   string    `json:"exchange"`
	Instrument string    `json:"instrument"`
	Class      string    `json:"class"`
	Timestamp  time.Time `json:"timestamp"`
	ModelKey   string    `json:"model_key"`

	ZIntertickFast float64 `json:"z_intertick_fast"`
	ZSpreadFast    float64 `json:"z_spread_fast"`
	ZPriceStepFast float64 `json:"z_price_step_fast"`
	ZVolumeFast    float64 `json:"z_volume_fast"`

	ZIntertickSlow float64 `json:"z_intertick_slow"`
	ZSpreadSlow    float64 `json:"z_spread_slow"`
	ZPriceStepSlow float64 `json:"z_price_step_slow"`
	ZVolumeSlow    float64 `json:"z_volume_slow"`

	CusumIntertick float64 `json:"cusum_intertick"`
	CusumSpread    float64 `json:"cusum_spread"`
	CusumPriceStep float64 `json:"cusum_price_step"`
	CusumVolume    float64 `json:"cusum_volume"`

	GapFlag  int `json:"gap_flag"`  // 1 if interval > 5x rolling mean
	QuoteInv int `json:"quote_inv"` // 1 if bid >= ask

	WarmupFlag          int `json:"warmup_flag"`           // 1 if < 50 observations total
	SessionFallbackFlag int `json:"session_fallback_flag"` // 1 if used fallback instead of session stats

}
