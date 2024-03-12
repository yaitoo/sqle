package migrate

import "time"

func round(d time.Duration) time.Duration {
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
