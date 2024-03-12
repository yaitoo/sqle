package migrate

import (
	// skipcq: GSC-G501
	"crypto/md5"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

type Rotation struct {
	Name     string
	Checksum string
	Script   string
}

func getRotate(option string) shardid.TableRotate {
	switch strings.ToLower(option) {
	case "monthly":
		return shardid.MonthlyRotate
	case "weekly":
		return shardid.WeeklyRotate
	case "daily":
		return shardid.DailyRotate
	default:
		return shardid.NoRotate
	}
}

func getRotateTime(option string) (time.Time, error) {
	year, err := strconv.Atoi(string(option[0:4]))
	if err != nil {
		return time.Time{}, err
	}

	month, err := strconv.Atoi(string(option[4:6]))
	if err != nil {
		return time.Time{}, err
	}

	day, err := strconv.Atoi(string(option[6:8]))
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

func getRotateRange(option string) (time.Time, time.Time, error) {
	r := strings.Split(option, "-")
	if len(r) == 2 {
		b, err := getRotateTime(r[0])
		if err != nil {
			return time.Time{}, time.Time{}, err
		}

		e, err := getRotateTime(r[1])
		if err != nil {
			return time.Time{}, time.Time{}, err
		}

		return b, e, nil
	}

	return time.Time{}, time.Time{}, ErrInvalidRotateRange
}

func loadRotations(fsys fs.FS, d string) ([]Rotation, error) {
	files, err := fs.ReadDir(fsys, d)
	if err != nil {
		return nil, err
	}

	var items []Rotation
	var s string

	for _, di := range files {
		dn := di.Name()
		buf, err := fs.ReadFile(fsys, filepath.Join(d, dn))
		if err != nil {
			return nil, err
		}

		s = string(buf)
		if strings.Contains(s, "<rotate>") {
			var it Rotation

			it.Name = dn[0 : len(dn)-len(filepath.Ext(dn))]
			it.Script = s

			// skipcq: GSC-G401, GO-S1023
			h := md5.New()
			h.Write(buf)

			it.Checksum = fmt.Sprintf("%x", h.Sum(nil))

			items = append(items, it)
		}
	}

	return items, nil
}
