package sqle

// LimitOption is a function type that modifies a LimitedQuery.
type LimitOption func(q *LimitOptions)

// LimitOptions represents a query with pagination and ordering options.
type LimitOptions struct {
	Offset  int64           // Offset represents the number of rows to skip before returning the result set.
	Limit   int64           // Limit represents the maximum number of results to be returned.
	OrderBy *OrderByBuilder // OrderBy represents the ordering of the query.
}

// WithPageSize is a LimitedQueryOption that sets the page size of the LimitedQuery.
func WithPageSize(size int64) LimitOption {
	return func(q *LimitOptions) {
		q.Limit = size
	}
}

// WithOrderBy is a LimitedQueryOption that sets the ordering of the LimitedQuery.
func WithOrderBy(ob *OrderByBuilder) LimitOption {
	return func(q *LimitOptions) {
		q.OrderBy = ob
	}
}
