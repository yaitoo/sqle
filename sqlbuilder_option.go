package sqle

type BuilderOption func(opts *BuilderOptions)

type BuilderOptions struct {
	ToSnake func(string) string // db column name converter
	Columns []string            // db column filter
}

func WithToSnake(fn func(string) string) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.ToSnake = fn
	}
}

// WithFiler only allowed columns can be written to db
func WithAllow(columns []string) BuilderOption {
	return func(opts *BuilderOptions) {

		for _, c := range columns {
			opts.Columns = append(opts.Columns, c)
		}
	}
}
