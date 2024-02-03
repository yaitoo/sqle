package sqle

type BuilderOption func(opts *BuilderOptions)

type BuilderOptions struct {
	ToSnake func(string) string
}

func WithToSnake(fn func(string) string) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.ToSnake = fn
	}
}
