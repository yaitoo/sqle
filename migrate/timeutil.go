package migrate

import "time"

// roundDuration rounds d down to a coarse unit (second / millisecond /
// microsecond) for human-readable elapsed-time logging. It is named
// roundDuration (rather than `round`) so it does not visually collide
// with time.Duration's built-in Round method in IDE auto-complete.
func roundDuration(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(time.Second)
	case d > time.Millisecond:
		return d.Round(time.Millisecond)
	case d > time.Microsecond:
		return d.Round(time.Microsecond)
	default:
		return d
	}
}
