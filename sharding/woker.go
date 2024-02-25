package sharding

import (
	"os"
	"strconv"
)

func acquireWorkerID() int8 {

	i, err := strconv.Atoi(os.Getenv("SQLE_WORKER_ID"))
	if err != nil {
		return 0
	}

	if i >= 0 && i <= int(MaxWorkerID) {
		return int8(i)
	}

	return 0
}
