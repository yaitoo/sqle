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

type Migrator struct {
	db     *sqle.DB
	suffix string

	Versions []Semver
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

func New(db *sqle.DB) *Migrator {
	return &Migrator{
		db:       db,
		suffix:   ".sql",
		Versions: make([]Semver, 0, 25),
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
			matches := regexpSemver.FindStringSubmatch(d.Name())

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

					mi := Migration{}

					matches := regexpChange.FindStringSubmatch(name)
					if matches == nil {
						return ErrInvalidScriptName
					}

					o, err := strconv.Atoi(matches[1])
					if err != nil {
						return ErrInvalidScriptName
					}

					mi.Rank = o
					mi.Name = matches[2]

					buf, err := fs.ReadFile(fsys, filepath.Join(path, name))
					if err != nil {
						return err
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

					v.Migrations = append(v.Migrations, mi)
				}

				if err != nil {
					return err
				}

				sort.Sort(&v)
				m.Versions = append(m.Versions, v)
				return nil
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
	_, err := m.db.ExecContext(ctx, TABLE_MIGRATIONS)

	return err
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
	log.Println("migrate:")

	for _, v := range m.Versions {
		n := len(v.Migrations)
		w := len(strconv.Itoa(n))
		log.Printf("┌─[ v%s ]\n", v.Name)
		err = m.db.Transaction(ctx, nil, func(ctx context.Context, tx *sqle.Tx) error {

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
						rotations = append(rotations, "_"+t.Format("200601"))
					}
				case shardid.WeeklyRotate:
					var week int
					for t := s.RotateBegin; !t.After(s.RotateEnd); t = t.AddDate(0, 0, 7) {
						_, week = t.ISOWeek() //1-53 week
						rotations = append(rotations, "_"+t.Format("2006")+fmt.Sprintf("%03d", week))
					}

				case shardid.DailyRotate:
					for t := s.RotateBegin; !t.After(s.RotateEnd); t = t.AddDate(0, 0, 1) {
						rotations = append(rotations, "_"+t.Format("20060102"))
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
