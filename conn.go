package sqle

import "context"

// Conn represents a database connection. Context and Tx both implement this interface.
type Conn interface {
	// Query executes a query that returns multiple rows.
	// It takes a query string and optional arguments.
	// It returns a pointer to a Rows object and an error, if any.
	Query(query string, args ...any) (*Rows, error)

	// QueryBuilder executes a query using a Builder object.
	// It takes a context and a Builder object.
	// It returns a pointer to a Rows object and an error, if any.
	QueryBuilder(ctx context.Context, b *Builder) (*Rows, error)

	// QueryContext executes a query that returns multiple rows using a context.
	// It takes a context, a query string, and optional arguments.
	// It returns a pointer to a Rows object and an error, if any.
	QueryContext(ctx context.Context, query string, args ...any) (*Rows, error)

	// QueryRow executes a query that returns a single row.
	// It takes a query string and optional arguments.
	// It returns a pointer to a Row object.
	QueryRow(query string, args ...any) *Row

	// QueryRowBuilder executes a query that returns a single row using a Builder object.
	// It takes a context and a Builder object.
	// It returns a pointer to a Row object.
	QueryRowBuilder(ctx context.Context, b *Builder) *Row

	// QueryRowContext executes a query that returns a single row using a context.
	// It takes a context, a query string, and optional arguments.
	// It returns a pointer to a Row object.
	QueryRowContext(ctx context.Context, query string, args ...any) *Row
}
