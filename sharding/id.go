package sharding

import (
	"fmt"
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

	MaxSequence   int16 = -1 ^ (-1 << SequenceBits) //1023
	MaxTableShard int8  = -1 ^ (-1 << TableBits)
	MaxDatabaseID int16 = -1 ^ (-1 << DatabaseBits)
	MaxWorkerID   int8  = -1 ^ (-1 << WorkerBits)
	MaxTimeMillis int64 = -1 ^ (-1 << TimeMillisBits)
)

type TableRotate int8

var (
	None    TableRotate = 0
	Monthly TableRotate = 1
	Weekly  TableRotate = 2
	Daily   TableRotate = 3
)

type ID struct {
	Time       time.Time
	ID         int64
	TimeMillis int64

	Sequence   int16
	DatabaseID int16

	WorkerID    int8
	TableRotate TableRotate
}

func (i *ID) RotateName() string {
	switch i.TableRotate {
	case Daily:
		return i.Time.Format("20060102")
	case Weekly:
		_, week := i.Time.ISOWeek() //1-53 week
		return i.Time.Format("2006") + fmt.Sprintf("%03d", week)
	case Monthly:
		return i.Time.Format("200601")
	default:
		return ""
	}
}

func Build(timeNow int64, workerID int8, databaseID int16, tr TableRotate, sequence int16) int64 {
	return int64(timeNow-TimeEpoch)<<TimeNowShift | int64(workerID)<<WorkerShift | int64(databaseID)<<DatabaseShift | int64(tr)<<TableShift | int64(sequence)
}

func Parse(id int64) ID {
	s := ID{
		ID:          id,
		Sequence:    int16(id) & MaxSequence,
		TableRotate: TableRotate(int8(id>>TableShift) & MaxTableShard),
		DatabaseID:  int16(id>>DatabaseShift) & MaxDatabaseID,
		WorkerID:    int8(id>>WorkerShift) & MaxWorkerID,
		TimeMillis:  int64(id>>TimeNowShift)&MaxTimeMillis + TimeEpoch,
	}
	s.Time = time.UnixMilli(s.TimeMillis).UTC()

	return s
}
