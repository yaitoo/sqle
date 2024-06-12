package sqle

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
)

var nullJsonBytes = []byte("null")

const nullJson = "null"

type Null[T any] struct {
	sql.Null[T]
}

func NewNull[T any](v T, valid bool) Null[T] {
	return Null[T]{Null: sql.Null[T]{V: v, Valid: valid}}
}

// Scan implements the [sql.Scanner] interface.
func (t *Null[T]) Scan(value any) error { // skipcq: GO-W1029
	return t.Null.Scan(value)
}

// Value implements the [driver.Valuer] interface.
func (t Null[T]) Value() (driver.Value, error) { // skipcq: GO-W1029
	return t.Null.Value()
}

// TValue returns the underlying value of the Null struct.
func (t *Null[T]) TValue() T { // skipcq: GO-W1029
	return t.Null.V
}

// MarshalJSON implements the json.Marshaler interface
func (t Null[T]) MarshalJSON() ([]byte, error) { // skipcq: GO-W1029
	if t.Valid {
		return json.Marshal(t.Null.V)
	}
	return nullJsonBytes, nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (t *Null[T]) UnmarshalJSON(data []byte) error { // skipcq: GO-W1029
	if len(data) == 0 || string(data) == nullJson {
		t.Null.Valid = false
		return nil
	}

	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	t.Null.V = v
	t.Null.Valid = true

	return nil
}
