package sharding

import (
	"time"
)

const (

	// TimeEpoch : 2024-2-19
	TimeEpoch int64 = 1708300800000
	// TimesNowBits milliseconds since TimeEpoch
	TimesNowBits = 39
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

	SequenceMask  int16 = -1 ^ (-1 << SequenceBits) //1024
	MaxTableShard int8  = -1 ^ (-1 << TableBits)
	MaxDatabaseID int16 = -1 ^ (-1 << DatabaseBits)
	MaxWorkerID   int8  = -1 ^ (-1 << WorkerBits)
	MaxTimeNow    int64 = -1 ^ (-1 << TimesNowBits)
)

type TableShard int8

var (
	TableShardNone  TableShard = 0
	TableShardMonth TableShard = 1
	TableShardWeek  TableShard = 2
	TableShardDay   TableShard = 3
)

type ID struct {
	Time      time.Time
	ID        int64
	TimeNow   int64
	TableName string

	Sequence   int16
	DatabaseID int16

	WorkerID   int8
	TableShard TableShard
}

func Build(timeNow int64, workerID int8, databaseID int16, table TableShard, sequence int16) int64 {
	return int64(timeNow-TimeEpoch)<<TimeNowShift | int64(workerID)<<WorkerShift | int64(databaseID)<<DatabaseShift | int64(table)<<TableShift | int64(sequence)
}

func From(id int64) ID {
	s := ID{
		ID:         id,
		Sequence:   int16(id) & SequenceMask,
		TableShard: TableShard(int8(id>>TableShift) & MaxTableShard),
		DatabaseID: int16(id>>DatabaseShift) & MaxDatabaseID,
		WorkerID:   int8(id>>WorkerShift) & MaxWorkerID,
		TimeNow:    int64(id>>TimeNowShift) & MaxTimeNow,
	}
	s.Time = time.UnixMilli(int64(s.TimeNow + TimeEpoch)).UTC()
	return s
}
