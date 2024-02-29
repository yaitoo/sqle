package migrate

import (
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

func (m *Migrator) Init(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, TABLE_MIGRATIONS)

	return err
}

func (m *Migrator) Migrate(ctx context.Context) error {
	var err error
	log.Printf("migrate:\n")
	for _, v := range m.Versions {
		n := len(v.Migrations)
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
					log.Printf("│ »[%d/%d] %-35s [✔]", i+1, n, s.Name)
					continue
				}

				now := time.Now()
				for _, it := range strings.Split(s.Scripts, ";") {
					it = strings.TrimSpace(it)
					if it != "" {
						_, err = tx.Exec(it + ";")
						if err != nil {
							return err
						}
					}
				}

				cmd := sqle.New()

				cmd.Insert("sqle_migrations").
					Set("checksum", s.Checksum).
					Set("version", v.Name).
					Set("name", s.Name).
					Set("rank", s.Rank).
					Set("scripts", s.Scripts).
					Set("migrated_on", now).
					Set("execution_time", time.Since(now).String()).
					End()

				query, args, err := cmd.Build()
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(ctx, query, args...)
				if err != nil {
					return err
				}

				log.Printf("│ »[%d/%d] %-35s [+]\n", i+1, n, s.Name)
			}

			log.Println("└───────────────────────────────────────────────")
			return nil
		})

		if err != nil {
			return err
		}

	}

	return nil
}
