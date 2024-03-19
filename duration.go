package sqle

import (
	"database/sql/driver"
	"errors"
	"time"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration { // skipcq: GO-W1029
	return time.Duration(d)
}

// Value implements the driver.Valuer interface,
// and turns the Duration into a VARCHAR field  for MySQL storage.
func (d Duration) Value() (driver.Value, error) { // skipcq: GO-W1029
	return time.Duration(d).String(), nil
}

// Scan implements the sql.Scanner interface,
// and turns the VARCHAR field incoming from MySQL into a Duration
func (d *Duration) Scan(src interface{}) error { // skipcq: GO-W1029
	if src == nil {
		return nil
	}

	var val string

	switch v := src.(type) {
	case []byte:
		val = string(v)
	case string:
		val = v
	default:
		return errors.New("bad duration type assertion")
	}

	td, err := time.ParseDuration(val)
	if err != nil {
		return err
	}

	*d = Duration(td)

	return nil
}
