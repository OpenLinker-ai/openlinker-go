package openlinker

import "math"

func clampInt64ToInt32(value int64) int32 {
	if value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- value is clamped into int32 range above.
	return int32(value)
}

func clampIntToInt32(value int) int32 {
	if value < 0 {
		return 0
	}
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	// #nosec G115 -- value is clamped into int32 range above.
	return int32(value)
}
