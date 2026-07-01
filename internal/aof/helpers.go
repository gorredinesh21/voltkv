package aof

import "time"

// msUntil returns the number of milliseconds from now until t (>=0).
func msUntil(t time.Time) int64 {
	d := time.Until(t)
	if d < 0 {
		return 0
	}
	return int64(d / time.Millisecond)
}
