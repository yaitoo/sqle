package sqle

import (
	"database/sql/driver"
	"encoding/json"
)

type String struct {
	Null[string]
}

func NewString(s string) String {
	return String{Null: NewNull(s, true)}
}

// Scan implements the [sql.Scanner] interface.
func (t *String) Scan(value any) error { // skipcq: GO-W1029
	return t.Null.Scan(value)
}

// Value implements the [driver.Valuer] interface.
func (t String) Value() (driver.Value, error) { // skipcq: GO-W1029
	return t.Null.Value()
}

// Time returns the underlying time.Time value of the Time struct.
func (t *String) String() string { // skipcq: GO-W1029
	return t.TValue()
}

// MarshalJSON implements the json.Marshaler interface
func (t String) MarshalJSON() ([]byte, error) { // skipcq: GO-W1029
	if t.Valid {
		return json.Marshal(t.TValue())
	}
	return nullJsonBytes, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (t *String) UnmarshalJSON(data []byte) error { // skipcq: GO-W1029
	if len(data) == 0 || string(data) == nullJson {
		t.Null.Valid = false
		return nil
	}

	var v string
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	t.Null.V = v
	t.Null.Valid = true

	return nil
}
