package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yaitoo/sqle"
	"github.com/yaitoo/sqle/shardid"
)

var (
	regexpSemver = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
	regexpChange = regexp.MustCompile(`^(\d+)_([0-9a-z_\-]+)\.`)
)

const TABLE_MIGRATIONS = "CREATE TABLE IF NOT EXISTS sqle_migrations(" +
	"checksum varchar(32) NOT NULL," +
	"module varchar(45) NOT NULL," +
	"version varchar(45) NOT NULL," +
	"name varchar(45) NOT NULL," +
	"rank int NOT NULL DEFAULT '0'," +
	"migrated_on datetime NOT NULL," +
	"execution_time varchar(25) NOT NULL," +
	"scripts text NOT NULL," +
	"PRIMARY KEY (checksum));"

const TABLE_ROTATIONS = "CREATE TABLE IF NOT EXISTS sqle_rotations(" +
	"checksum varchar(32) NOT NULL," +
	"rotated_name varchar(8) NOT NULL," +
	"name varchar(45) NOT NULL," +
	"rotated_on datetime NOT NULL," +
	"execution_time varchar(25) NOT NULL," +
	"PRIMARY KEY (checksum, rotated_name));"

type MigrationStatus int

const (
	MigrationStatusNew      MigrationStatus = iota // new migration, needs to be executed
	MigrationStatusExecuted                        // already executed, checksum matches
	MigrationStatusModified                        // already executed but checksum changed
)

type Migrator struct {
	dbs    []*sqle.DB
	suffix string
	module string

	Versions         []Semver
	MonthlyRotations []Rotation
	WeeklyRotations  []Rotation
	DailyRotations   []Rotation

	now func() time.Time
}

func (s *Migrator) Len() int      { return len(s.Versions) }
func (s *Migrator) Swap(i, j int) { s.Versions[i], s.Versions[j] = s.Versions[j], s.Versions[i] }
func (s *Migrator) Less(i, j int) bool {
	l := s.Versions[i]
	r := s.Versions[j]

	if l.Major < r.Major {
		return true
	}

	if l.Major > r.Major {
		return false
	}

	// Major == Major
	if l.Minor < r.Minor {
		return true
	}

	if l.Minor > r.Minor {
		return false
	}

	// Minor == Minor
	if l.Patch < r.Patch {
		return true
	}

	if l.Patch > r.Patch {
		return false
	}

	// Patch == Patch

	// simply compare prerelease
	return l.Prerelease < r.Prerelease

}

func New(dbs ...*sqle.DB) *Migrator {
	return &Migrator{
		dbs:      dbs,
		suffix:   ".sql",
		Versions: make([]Semver, 0, 25),
		now:      time.Now,
	}
}

func (m *Migrator) Discover(fsys fs.FS, options ...Option) error {
	for _, option := range options {
		option(m)
	}
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dn := d.Name()
			matches := regexpSemver.FindStringSubmatch(dn)

			// full/Major/Minor/Patch/Prerelease/Build
			if len(matches) == 6 {
				major, _ := strconv.Atoi(matches[1])
				minor, _ := strconv.Atoi(matches[2])
				patch, _ := strconv.Atoi(matches[3])

				err = m.loadVersion(matches[0], major, minor, patch, matches[4], matches[5], fsys, path)
				if err != nil {
					return err
				}
				return nil
			} else if dn == "monthly" {
				m.MonthlyRotations, err = loadRotations(fsys, path)
				if err != nil {
					return err
				}
			} else if dn == "weekly" {
				m.WeeklyRotations, err = loadRotations(fsys, path)
				if err != nil {
					return err
				}
			} else if dn == "daily" {
				m.DailyRotations, err = loadRotations(fsys, path)
				if err != nil {
					return err
				}
			}

		}

		return nil
	})

	if err != nil {
		return err
	}

	sort.Sort(m)

	return nil
}

func (m *Migrator) loadVersion(name string, major int, minor int, patch int, prerelease, build string, fsys fs.FS, path string) error {
	v := Semver{
		Name:       name,
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		Prerelease: prerelease,
		Build:      build,
	}
	files, err := fs.ReadDir(fsys, path)
	if err != nil {
		return err
	}

	for _, di := range files {
		if di.IsDir() {
			continue
		}

		name := di.Name()
		if !strings.HasSuffix(name, m.suffix) {
			continue
		}

		mi, err := loadMigration(name, fsys, path)
		if err != nil {
			return err
		}

		v.Migrations = append(v.Migrations, mi)
	}

	sort.Sort(&v)
	m.Versions = append(m.Versions, v)
	return nil
}

func (m *Migrator) Init(ctx context.Context) error {
	for _, db := range m.dbs {
		_, err := db.ExecContext(ctx, TABLE_MIGRATIONS)
		if err != nil {
			return err
		}

		_, err = db.ExecContext(ctx, TABLE_ROTATIONS)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) Migrate(ctx context.Context) error {
	var err error
	n := len(m.dbs)
	for i, db := range m.dbs {
		if n == 1 {
			log.Printf("migrate: %s\n", m.module)
		} else {
			log.Printf("migrate db-%v: %s\n", i, m.module)
		}

		err = m.startMigrate(ctx, db)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) startMigrate(ctx context.Context, db *sqle.DB) error {
	var err error

	for _, v := range m.Versions {
		n := len(v.Migrations)
		w := len(strconv.Itoa(n))
		log.Printf("┌─[ v%s ]\n", v.Name)
		err = db.Transaction(ctx, nil, func(ctx context.Context, tx *sqle.Tx) error {

			for i, s := range v.Migrations {
				status, err := m.getMigrationStatus(tx, v.Name, s)
				if err != nil {
					return err
				}

				if status == MigrationStatusExecuted {
					log.Printf("│ »[%*d/%d] %-35s %-10s [✔]", w, i+1, n, s.Name, "")
					continue
				}

				if status == MigrationStatusModified {
					log.Printf("│ »[%*d/%d] %-35s %-10s [!]", w, i+1, n, s.Name, "")
					continue
				}

				rotations := m.buildRotations(s.Rotate, s.RotateBegin, s.RotateEnd)

				now := time.Now()
				for _, it := range strings.Split(s.Scripts, ";") {
					it := strings.TrimSpace(it)
					if it != "" {
						for _, rt := range rotations {
							s := strings.ReplaceAll(it, "<rotate>", rt)
							_, err = tx.Exec(s + ";")
							if err != nil {
								return err
							}
						}
					}
				}

				cmd := sqle.New()
				et := round(time.Since(now)).String()
				cmd.Insert("sqle_migrations").
					Set("checksum", s.Checksum).
					Set("module", m.module).
					Set("version", v.Name).
					Set("name", s.Name).
					Set("rank", s.Rank).
					Set("scripts", s.Scripts).
					Set("migrated_on", now).
					Set("execution_time", et).
					End()

				query, args, err := cmd.Build()
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, query, args...)
				if err != nil {
					return err
				}

				log.Printf("│ »[%*d/%d] %-35s %-10s [+]\n", w, i+1, n, s.Name, et)
				if len(rotations) > 1 {
					log.Printf("│ »      %-35s \n", "rotate:")
					for _, rt := range rotations[1:] {
						log.Printf("│ »       + %-35s \n", rt)
					}
				}

			}

			log.Println("└────────────────────────────────────────────────────────────────")
			return nil
		})

		if err != nil {
			return err
		}

	}

	return nil
}

func (m *Migrator) getMigrationStatus(tx *sqle.Tx, version string, s Migration) (MigrationStatus, error) {
	// First check if checksum already exists (most common case: script already executed)
	var checksum string
	err := tx.QueryRow("SELECT checksum FROM sqle_migrations WHERE checksum = ?",
		s.Checksum).Scan(&checksum)
	if err == nil {
		// Checksum exists, meaning a script with same content was already executed
		return MigrationStatusExecuted, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MigrationStatusNew, err
	}

	// Checksum doesn't exist, check if a script with same name and rank was modified
	err = tx.QueryRow("SELECT checksum FROM sqle_migrations WHERE module = ? AND version = ? AND name = ? AND rank = ?",
		m.module, version, s.Name, s.Rank).Scan(&checksum)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No record with same name and rank exists, it's a new script
			return MigrationStatusNew, nil
		}
		return MigrationStatusNew, err
	}

	// Record with same name and rank exists but checksum is different, script content was modified
	return MigrationStatusModified, nil
}

func (*Migrator) buildRotations(r shardid.TableRotate, begin, end time.Time) []string {
	rotations := []string{""}
	switch r {
	case shardid.MonthlyRotate:
		for t := begin; !t.After(end); t = t.AddDate(0, 1, 0) {
			rotations = append(rotations, shardid.FormatMonth(t))
		}
	case shardid.WeeklyRotate:
		for t := begin; !t.After(end); t = t.AddDate(0, 0, 7) {
			rotations = append(rotations, shardid.FormatWeek(t))
		}

	case shardid.DailyRotate:
		for t := begin; !t.After(end); t = t.AddDate(0, 0, 1) {
			rotations = append(rotations, shardid.FormatDay(t))
		}
	}
	return rotations
}

func (m *Migrator) Rotate(ctx context.Context) error {
	var err error
	n := len(m.dbs)
	for i, db := range m.dbs {
		if n == 1 {
			log.Printf("rotate: %s\n", m.module)
		} else {
			log.Printf("rotate db-%v: %s\n", i, m.module)
		}

		now := m.now().UTC()
		months := []string{
			"_" + now.Format("200601"),
			"_" + now.AddDate(0, 1, 0).Format("200601"),
		}

		err = startRotate(ctx, db, months, m.MonthlyRotations)
		if err != nil {
			return err
		}

		var week int
		_, week = now.ISOWeek() // 1-53 week

		next := now.AddDate(0, 0, 7)
		_, nextWeek := next.ISOWeek()

		weeks := []string{
			"_" + now.Format("2006") + fmt.Sprintf("%03d", week),
			"_" + next.Format("2006") + fmt.Sprintf("%03d", nextWeek),
		}

		err = startRotate(ctx, db, weeks, m.WeeklyRotations)
		if err != nil {
			return err
		}

		days := []string{
			"_" + now.Format("20060102"),
			"_" + now.AddDate(0, 0, 1).Format("20060102"),
		}

		err = startRotate(ctx, db, days, m.DailyRotations)
		if err != nil {
			return err
		}

	}

	return nil
}

func startRotate(ctx context.Context, db *sqle.DB, rotatedNames []string, rotations []Rotation) error {
	var err error
	var n int
	var w int
	var checksum string
	for _, r := range rotations {
		err = db.Transaction(ctx, nil, func(ctx context.Context, tx *sqle.Tx) error {
			n = len(rotatedNames)
			w = len(strconv.Itoa(n))
			log.Printf("┌─[ %s ]\n", r.Name)

			for i, rn := range rotatedNames {
				err = tx.QueryRow("SELECT checksum FROM sqle_rotations WHERE checksum = ? and rotated_name = ?", r.Checksum, rn).Scan(&checksum)
				if err != nil {
					if !errors.Is(err, sql.ErrNoRows) {
						return err
					}
				}

				if checksum != "" {
					log.Printf("│ »[%*d/%d] %-35s %-10s [✔]", w, i+1, n, rn, "")
					continue
				}

				now := time.Now()

				_, err = tx.Exec(strings.ReplaceAll(r.Script, "<rotate>", rn) + ";")
				if err != nil {
					return err
				}

				cmd := sqle.New()
				et := round(time.Since(now)).String()
				cmd.Insert("sqle_rotations").
					Set("checksum", r.Checksum).
					Set("name", r.Name).
					Set("rotated_name", rn).
					Set("rotated_on", now).
					Set("execution_time", et).
					End()

				query, args, err := cmd.Build()
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, query, args...)
				if err != nil {
					return err
				}

				log.Printf("│ »[%*d/%d] %-35s %-10s [+]\n", w, i+1, n, rn, et)
			}

			log.Println("└────────────────────────────────────────────────────────────────")
			return nil
		})

		if err != nil {
			return err
		}

	}

	return nil
}
