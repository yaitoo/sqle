package migrate

import (
	"bufio"
	// skipcq: GSC-G501
	"crypto/md5"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/yaitoo/sqle/shardid"
)

var (
	ErrInvalidScriptName  = errors.New("migrate: invalid script name")
	ErrInvalidRotateRange = errors.New("migrate: invalid rotate range")
)

type Semver struct {
	Name       string
	Major      int
	Minor      int
	Patch      int
	Prerelease string
	Build      string
	Migrations []Migration
}

func (s *Semver) Len() int      { return len(s.Migrations) }
func (s *Semver) Swap(i, j int) { s.Migrations[i], s.Migrations[j] = s.Migrations[j], s.Migrations[i] }
func (s *Semver) Less(i, j int) bool {
	return s.Migrations[i].Rank < s.Migrations[j].Rank
}

type Migration struct {
	Name     string
	Rank     int
	Checksum string
	Scripts  string

	Rotate      shardid.TableRotate
	RotateBegin time.Time
	RotateEnd   time.Time

	MigratedOn    *time.Time
	ExecutionTime time.Duration
}

func loadMigration(name string, fsys fs.FS, path string) (Migration, error) {
	mi := Migration{}

	matches := regexpChange.FindStringSubmatch(name)
	if matches == nil {
		return Migration{}, ErrInvalidScriptName
	}

	o, err := strconv.Atoi(matches[1])
	if err != nil {
		return Migration{}, ErrInvalidScriptName
	}

	mi.Rank = o
	mi.Name = matches[2]

	buf, err := fs.ReadFile(fsys, filepath.Join(path, name))
	if err != nil {
		return Migration{}, err
	}

	// skipcq: GSC-G401, GO-S1023
	h := md5.New()
	h.Write(buf)

	mi.Checksum = fmt.Sprintf("%x", h.Sum(nil))
	mi.Scripts = string(buf)

	s := bufio.NewScanner(strings.NewReader(mi.Scripts))
	s.Split(bufio.ScanLines)

	if s.Scan() {
		l := strings.ReplaceAll(s.Text(), " ", "")

		/*rotate:monthly=yyyyMMDD-yyyyMMDD*/
		if strings.HasPrefix(l, "/*") && strings.HasSuffix(l, "*/") {
			items := strings.Split(l, ":")
			if len(items) == 2 && items[0] == "/*rotate" {
				options := strings.Split(items[1][0:len(items[1])-2], "=")
				if len(options) == 2 && len(options[1]) == 17 {
					r := getRotate(options[0])
					if r != shardid.NoRotate {
						begin, end, err := getRotateRange(options[1])
						if err == nil {
							mi.Rotate = r
							mi.RotateBegin = begin
							mi.RotateEnd = end
						}
					}
				}
			}
		}

	}
	return mi, nil
}
