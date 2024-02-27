package shardid

import "time"

func Build(timeNow int64, workerID int8, databaseID int16, tr TableRotate, sequence int16) ID {
	id := int64(timeNow-TimeEpoch)<<TimeNowShift | int64(workerID)<<WorkerShift | int64(databaseID)<<DatabaseShift | int64(tr)<<TableShift | int64(sequence)

	return Parse(id)
}

func Parse(id int64) ID {
	s := ID{
		Int64:       id,
		Sequence:    int16(id) & MaxSequence,
		TableRotate: TableRotate(int8(id>>TableShift) & MaxTableShard),
		DatabaseID:  int16(id>>DatabaseShift) & MaxDatabaseID,
		WorkerID:    int8(id>>WorkerShift) & MaxWorkerID,
		TimeMillis:  int64(id>>TimeNowShift)&MaxTimeMillis + TimeEpoch,
	}
	s.Time = time.UnixMilli(s.TimeMillis).UTC()

	return s
}
