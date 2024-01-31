package migrate

import (
	"errors"
	"time"
)

var ErrInvalidScriptName = errors.New("migrate: invalid script name")

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
	Name          string
	Rank          int
	Checksum      string
	Scripts       string
	MigratedOn    *time.Time
	ExecutionTime time.Duration
}
