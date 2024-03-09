package sqle

import "context"

type Queryer[T any] interface {
	First(ctx context.Context, tables []string, b *Builder) (T, error)
	Count(ctx context.Context, tables []string, b *Builder) (int, error)
	Query(ctx context.Context, tables []string, b *Builder, less func(i, j T) bool) ([]T, error)
	QueryLimit(ctx context.Context, tables []string, b *Builder, less func(i, j T) bool, limit int) ([]T, error)
}
