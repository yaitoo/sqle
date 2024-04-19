package sqle

// LimitedQueryOption is a function type that modifies a LimitedQuery.
type LimitedQueryOption func(q *LimitedQuery)

// LimitedQuery represents a query with pagination and ordering options.
type LimitedQuery struct {
	Offset  int64           // PageIndex represents the index of the page to retrieve.
	Limit   int64           // PageSize represents the number of items per page.
	OrderBy *OrderByBuilder // OrderBy represents the ordering of the query.
}

// WithPageSize is a LimitedQueryOption that sets the page size of the LimitedQuery.
func WithPageSize(size int64) LimitedQueryOption {
	return func(q *LimitedQuery) {
		q.Limit = size
	}
}

// WithOrderBy is a LimitedQueryOption that sets the ordering of the LimitedQuery.
func WithOrderBy(ob *OrderByBuilder) LimitedQueryOption {
	return func(q *LimitedQuery) {
		q.OrderBy = ob
	}
}
