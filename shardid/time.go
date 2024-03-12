package shardid

import (
	"fmt"
	"time"
)

func FormatMonth(t time.Time) string {
	return t.Format("_200601")
}

func FormatWeek(t time.Time) string {
	_, week := t.ISOWeek() // 1-53 week
	return t.Format("_2006") + fmt.Sprintf("%03d", week)
}

func FormatDay(t time.Time) string {
	return t.Format("_20060102")
}
