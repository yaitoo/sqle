package migrate

import "strings"

type Option func(m *Migrator)

func WithSuffix(suffix string) Option {
	return func(m *Migrator) {
		if suffix != "" {
			if !strings.HasPrefix(suffix, ".") {
				m.suffix = "." + suffix
			} else {
				m.suffix = suffix
			}
		}
	}
}
