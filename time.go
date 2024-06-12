package sqle

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"
)

// Time represents a nullable time value.
type Time struct {
	sql.NullTime
}

// NewTime creates a new Time object with the given time and valid flag.
func NewTime(t time.Time, valid bool) Time {
	return Time{NullTime: sql.NullTime{Time: t, Valid: valid}}
}

// Scan implements the [sql.Scanner] interface.
func (t *Time) Scan(value any) error { // skipcq: GO-W1029
	return t.NullTime.Scan(value)
}

// Value implements the [driver.Valuer] interface.
func (t Time) Value() (driver.Value, error) { // skipcq: GO-W1029
	return t.NullTime.Value()
}

// Time returns the underlying time.Time value of the Time struct.
func (t *Time) Time() time.Time { // skipcq: GO-W1029
	return t.NullTime.Time
}

// MarshalJSON implements the json.Marshaler interface
func (t Time) MarshalJSON() ([]byte, error) { // skipcq: GO-W1029
	if t.Valid {
		return json.Marshal(t.NullTime.Time)
	}
	return nullJsonBytes, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (t *Time) UnmarshalJSON(data []byte) error { // skipcq: GO-W1029
	if len(data) == 0 || string(data) == nullJson {
		t.NullTime.Time = time.Time{}
		t.NullTime.Valid = false
		return nil
	}

	var v time.Time
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	t.NullTime.Time = v
	t.NullTime.Valid = true

	return nil
}
