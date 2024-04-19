package sqle

// LimitResult represents a limited result set with items and total count.
type LimitResult[T any] struct {
	Items []T   `json:"items,omitempty"` // Items contains the limited result items.
	Total int64 `json:"total,omitempty"` // Total represents the total count.
}
