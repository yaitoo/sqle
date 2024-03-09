package migrate

import (
	"bufio"
	"context"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
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
	"`checksum` varchar(32) NOT NULL," +
	"`version` varchar(45) NOT NULL," +
	"`name` varchar(45) NOT NULL," +
	"`rank` int NOT NULL DEFAULT '0'," +
	"`migrated_on` datetime NOT NULL," +
	"`execution_time` varchar(25) NOT NULL," +
	"`scripts` text NOT NULL," +
	"PRIMARY KEY (checksum));"

const TABLE_ROTATIONS = "CREATE TABLE IF NOT EXISTS sqle_rotations(" +
	"`checksum` varchar(32) NOT NULL," +
	"`rotated_name` varchar(8) NOT NULL," +
	"`name` varchar(45) NOT NULL," +
	"`rotated_on` datetime NOT NULL," +
	"`execution_time` varchar(25) NOT NULL," +
	"PRIMARY KEY (checksum, rotated_name));"

type Migrator struct {
	dbs    []*sqle.DB
	suffix string

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

	//Major == Major
	if l.Minor < r.Minor {
		return true
	}

	if l.Minor > r.Minor {
		return false
	}

	//Minor == Minor
	if l.Patch < r.Patch {
		return true
	}

	if l.Patch > r.Patch {
		return false
	}

	//Patch == Patch

	//simply compare prerelease
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

			//full/Major/Minor/Patch/Prerelease/Build
			if len(matches) == 6 {

				major, _ := strconv.Atoi(matches[1])
				minor, _ := strconv.Atoi(matches[2])
				patch, _ := strconv.Atoi(matches[3])

				v := Semver{
					Name:       matches[0],
					Major:      major,
					Minor:      minor,
					Patch:      patch,
					Prerelease: matches[4],
					Build:      matches[5],
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

					mi, err := m.loadMigration(name, fsys, path)
					if err != nil {
						return err
					}

					v.Migrations = append(v.Migrations, mi)
				}

				sort.Sort(&v)
				m.Versions = append(m.Versions, v)
				return nil
			} else if dn == "monthly" {
				m.MonthlyRotations, err = m.loadRotations(fsys, path)
				if err != nil {
					return err
				}
			} else if dn == "weekly" {
				m.WeeklyRotations, err = m.loadRotations(fsys, path)
				if err != nil {
					return err
				}
			} else if dn == "daily" {
				m.DailyRotations, err = m.loadRotations(fsys, path)
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

func (m *Migrator) loadMigration(name string, fsys fs.FS, path string) (Migration, error) {
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
					r := m.getRotate(options[0])
					if r != shardid.NoRotate {
						begin, end, err := m.getRotateRange(options[1])
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

func (m *Migrator) loadRotations(fsys fs.FS, d string) ([]Rotation, error) {
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

			h := md5.New()
			h.Write(buf)

			it.Checksum = fmt.Sprintf("%x", h.Sum(nil))

			items = append(items, it)
		}
	}

	return items, nil
}

func (m *Migrator) getRotate(option string) shardid.TableRotate {
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

func (m *Migrator) getRotateTime(option string) (time.Time, error) {
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

func (m *Migrator) getRotateRange(option string) (time.Time, time.Time, error) {
	r := strings.Split(option, "-")
	if len(r) == 2 {
		b, err := m.getRotateTime(r[0])
		if err != nil {
			return time.Time{}, time.Time{}, err
		}

		e, err := m.getRotateTime(r[1])
		if err != nil {
			return time.Time{}, time.Time{}, err
		}

		return b, e, nil
	}

	return time.Time{}, time.Time{}, ErrInvalidRotateRange
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

func (m *Migrator) round(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(time.Second)
	case d > time.Millisecond:
		return d.Round(time.Millisecond)
	case d > time.Microsecond:
		return d.Round(time.Microsecond)
	default:
		return d
	}
}

func (m *Migrator) Migrate(ctx context.Context) error {
	var err error
	n := len(m.dbs)
	for i, db := range m.dbs {
		if n == 1 {
			log.Println("migrate:")
		} else {
			log.Printf("migrate db%v:\n", i)
		}

		err = m.migrate(ctx, db)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) migrate(ctx context.Context, db *sqle.DB) error {
	var err error

	for _, v := range m.Versions {
		n := len(v.Migrations)
		w := len(strconv.Itoa(n))
		log.Printf("┌─[ v%s ]\n", v.Name)
		err = db.Transaction(ctx, nil, func(ctx context.Context, tx *sqle.Tx) error {

			var checksum string

			for i, s := range v.Migrations {
				err = tx.QueryRow("SELECT `checksum` FROM `sqle_migrations` WHERE `checksum` = ?", s.Checksum).Scan(&checksum)
				if err != nil {
					if !errors.Is(err, sql.ErrNoRows) {
						return err
					}
				}

				if checksum != "" {
					log.Printf("│ »[%*d/%d] %-35s %-10s [✔]", w, i+1, n, s.Name, "")
					continue
				}

				rotations := []string{""}
				switch s.Rotate {
				case shardid.MonthlyRotate:
					for t := s.RotateBegin; !t.After(s.RotateEnd); t = t.AddDate(0, 1, 0) {
						rotations = append(rotations, shardid.FormatMonth(t))
					}
				case shardid.WeeklyRotate:
					for t := s.RotateBegin; !t.After(s.RotateEnd); t = t.AddDate(0, 0, 7) {
						rotations = append(rotations, shardid.FormatWeek(t))
					}

				case shardid.DailyRotate:
					for t := s.RotateBegin; !t.After(s.RotateEnd); t = t.AddDate(0, 0, 1) {
						rotations = append(rotations, shardid.FormatDay(t))
					}
				}

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
				et := m.round(time.Since(now)).String()
				cmd.Insert("sqle_migrations").
					Set("checksum", s.Checksum).
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

func (m *Migrator) Rotate(ctx context.Context) error {
	var err error
	n := len(m.dbs)
	for i, db := range m.dbs {
		if n == 1 {
			log.Println("rotate:")
		} else {
			log.Printf("rotate db%v:\n", i)
		}

		now := m.now().UTC()
		months := []string{
			"_" + now.Format("200601"),
			"_" + now.AddDate(0, 1, 0).Format("200601"),
		}

		err = m.rotate(ctx, db, months, m.MonthlyRotations)
		if err != nil {
			return err
		}

		var week int
		_, week = now.ISOWeek() //1-53 week

		next := now.AddDate(0, 0, 7)
		_, nextWeek := next.ISOWeek()

		weeks := []string{
			"_" + now.Format("2006") + fmt.Sprintf("%03d", week),
			"_" + next.Format("2006") + fmt.Sprintf("%03d", nextWeek),
		}

		err = m.rotate(ctx, db, weeks, m.WeeklyRotations)
		if err != nil {
			return err
		}

		days := []string{
			"_" + now.Format("20060102"),
			"_" + now.AddDate(0, 0, 1).Format("20060102"),
		}

		err = m.rotate(ctx, db, days, m.DailyRotations)
		if err != nil {
			return err
		}

	}

	return nil
}

func (m *Migrator) rotate(ctx context.Context, db *sqle.DB, rotatedNames []string, rotations []Rotation) error {
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
				err = tx.QueryRow("SELECT `checksum` FROM `sqle_rotations` WHERE `checksum` = ? and `rotated_name` = ?", r.Checksum, rn).Scan(&checksum)
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
				et := m.round(time.Since(now)).String()
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
