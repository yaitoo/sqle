package sqle

type BuilderOption func(opts *BuilderOptions)

type BuilderOptions struct {
	ToName  func(string) string // db column naming convert method
	Columns []string            // db column filter
}

func WithToName(fn func(string) string) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.ToName = fn
	}
}

// WithAllow only allowed columns can be written to db
func WithAllow(columns []string) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.Columns = append(opts.Columns, columns...)
	}
}
