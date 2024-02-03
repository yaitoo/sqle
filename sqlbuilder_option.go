package sqle

type BuilderOption func(opts *BuilderOptions)

type BuilderOptions struct {
	ToSnake   func(string) string // db column name converter
	DbColumns map[string]bool     // db column filter
}

func WithToSnake(fn func(string) string) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.ToSnake = fn
	}
}

func WithDbColumns(columns []string) BuilderOption {
	return func(opts *BuilderOptions) {
		if opts.DbColumns == nil {
			opts.DbColumns = map[string]bool{}
		}
		for _, c := range columns {
			opts.DbColumns[c] = true
		}
	}
}
