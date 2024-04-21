package shardid

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (

	// TimeEpoch : 2024-2-19
	TimeEpoch int64 = 1708300800000
	// TimeEnd : 2041-2-19
	TimeEnd int64 = 2244844800000

	// TimeMillisBits milliseconds since TimeEpoch
	TimeMillisBits = 39
	// WorkerBits worker id: 0-3
	WorkerBits = 2
	// DatabaseBits  database id: 0-1023
	DatabaseBits = 10
	// TableBits table sharding: 0=none/1=yyyyMM/2=yyyy0WW/3=yyyyMMDD
	TableBits = 2
	// SequenceBits sequence: 0-1023
	SequenceBits = 10

	TimeNowShift  = WorkerBits + DatabaseBits + TableBits + SequenceBits
	WorkerShift   = DatabaseBits + TableBits + SequenceBits
	DatabaseShift = TableBits + SequenceBits
	TableShift    = SequenceBits

	MaxSequence   int16 = -1 ^ (-1 << SequenceBits) // 1023
	MaxTableShard int8  = -1 ^ (-1 << TableBits)
	MaxDatabaseID int16 = -1 ^ (-1 << DatabaseBits)
	MaxWorkerID   int8  = -1 ^ (-1 << WorkerBits)
	MaxTimeMillis int64 = -1 ^ (-1 << TimeMillisBits)
)

// TableRotate table rotation option
type TableRotate int8

var (
	NoRotate      TableRotate = 0
	MonthlyRotate TableRotate = 1
	WeeklyRotate  TableRotate = 2
	DailyRotate   TableRotate = 3
)

// ID shardid info
type ID struct {
	Time       time.Time
	Int64      int64
	TimeMillis int64

	Sequence   int16
	DatabaseID int16

	WorkerID    int8
	TableRotate TableRotate
}

// RotateName format time parts as rotated table name suffix
func (i *ID) RotateName() string { // skipcq: GO-W1029
	switch i.TableRotate {
	case DailyRotate:
		return FormatDay(i.Time)
	case WeeklyRotate:
		return FormatWeek(i.Time)
	case MonthlyRotate:
		return FormatMonth(i.Time)
	default:
		return ""
	}
}

// Value implements the driver.Valuer interface
func (b ID) Value() (driver.Value, error) { // skipcq: GO-W1029
	return b.Int64, nil
}

// Scan implements the sql.Scanner interface,
func (b *ID) Scan(src interface{}) error { // skipcq: GO-W1029
	if src == nil {
		return nil
	}
	v, ok := src.(int64)
	if !ok {
		return errors.New("bad int64 type assertion")
	}
	id := Parse(v)

	b.DatabaseID = id.DatabaseID
	b.Int64 = v
	b.Sequence = id.Sequence
	b.TableRotate = id.TableRotate
	b.Time = id.Time
	b.TimeMillis = id.TimeMillis
	b.WorkerID = id.WorkerID
	return nil
}

// MarshalJSON implements the json.Marshaler interface
func (id ID) MarshalJSON() ([]byte, error) { // skipcq: GO-W1029
	return []byte(fmt.Sprintf(`"%d"`, id.Int64)), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (id *ID) UnmarshalJSON(data []byte) error { // skipcq: GO-W1029
	s := string(data)

	i, err := strconv.ParseInt(strings.Trim(s, "\""), 10, 64)
	if err != nil {
		return err
	}

	*id = Parse(i)
	return nil
}
