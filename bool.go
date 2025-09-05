package sqle

import (
	"database/sql/driver"
	"strings"
)

// Bool is an implementation of a bool for the MySQL type BIT(1).
type Bool bool

// Value implements the driver.Valuer interface,
// and turns the BitBool into a bit field (BIT(1)) for MySQL storage.
func (b Bool) Value() (driver.Value, error) { // skipcq: GO-W1029
	return bool(b), nil
}

// Scan implements the sql.Scanner interface,
// and turns the bit field incoming from MySQL into a BitBool
func (b *Bool) Scan(src interface{}) error { // skipcq: GO-W1029
	if src == nil {
		return nil
	}

	switch v := src.(type) {
	case []byte:
		*b = v[0] == 1
	case int64:
		*b = v == 1
	case bool:
		*b = Bool(v)
	case string:
		if v == "1" || strings.EqualFold(v, "t") || strings.EqualFold(v, "true") {
			*b = true
		} else {
			*b = false
		}
	}

	return nil
}
