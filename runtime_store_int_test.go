package openlinker

import (
	"math"
	"testing"
)

func TestClampInt64ToInt32(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int64
		want int32
	}{
		{name: "negative", in: -1, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "normal", in: 123, want: 123},
		{name: "max", in: math.MaxInt32, want: math.MaxInt32},
		{name: "overflow", in: int64(math.MaxInt32) + 1, want: math.MaxInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampInt64ToInt32(tc.in); got != tc.want {
				t.Fatalf("clampInt64ToInt32(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestClampIntToInt32(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   int
		want int32
	}{
		{name: "negative", in: -1, want: 0},
		{name: "zero", in: 0, want: 0},
		{name: "normal", in: 123, want: 123},
		{name: "max", in: math.MaxInt32, want: math.MaxInt32},
		{name: "overflow", in: int(math.MaxInt32) + 1, want: math.MaxInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampIntToInt32(tc.in); got != tc.want {
				t.Fatalf("clampIntToInt32(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
