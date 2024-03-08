package shardid

import (
	"fmt"
	"time"
)

func FormatMonth(t time.Time) string {
	return t.Format("200601")
}

func FormatWeek(t time.Time) string {
	_, week := t.ISOWeek() //1-53 week
	return t.Format("2006") + fmt.Sprintf("%03d", week)
}

func FormatDay(t time.Time) string {
	return t.Format("20060102")
}
