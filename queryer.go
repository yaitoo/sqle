package sqle

import "context"

// Queryer is a query provider interface that defines methods for querying data.
type Queryer[T any] interface {
	// First retrieves the first result that matches the query criteria.
	First(ctx context.Context, rotatedTables []string, b *Builder) (T, error)

	// Count returns the number of results that match the query criteria.
	Count(ctx context.Context, rotatedTables []string, b *Builder) (int, error)

	// Query retrieves all results that match the query criteria and sorts them using less function if it is provided.
	Query(ctx context.Context, rotatedTables []string, b *Builder, less func(i, j T) bool) ([]T, error)

	// QueryLimit retrieves a limited number of results that match the query criteria, sorts them using the provided less function, and limits the number of results to the specified limit.
	QueryLimit(ctx context.Context, rotatedTables []string, b *Builder, less func(i, j T) bool, limit int) ([]T, error)
}
