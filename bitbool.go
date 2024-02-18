package sqle

import (
	"database/sql/driver"
	"errors"
)

// BitBool is an implementation of a bool for the MySQL type BIT(1).
type BitBool bool

// Value implements the driver.Valuer interface,
// and turns the BitBool into a bit field (BIT(1)) for MySQL storage.
func (b BitBool) Value() (driver.Value, error) {
	if b {
		return []byte{1}, nil
	} else {
		return []byte{0}, nil
	}
}

// Scan implements the sql.Scanner interface,
// and turns the bit field incoming from MySQL into a BitBool
func (b *BitBool) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	v, ok := src.([]byte)
	if !ok {
		return errors.New("bad []byte type assertion")
	}
	*b = v[0] == 1
	return nil
}
