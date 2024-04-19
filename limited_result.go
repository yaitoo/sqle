package sqle

// LimitedResult represents a limited result set with items and total count.
type LimitedResult[T any] struct {
	Items []T   `json:"items,omitempty"` // Items contains the limited result items.
	Total int64 `json:"total,omitempty"` // Total represents the total count.
}
