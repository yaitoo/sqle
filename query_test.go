package sqle

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// myErr is a test-only error type used to verify errors.As across *Errors.
type myErr struct{ code int }

func (m *myErr) Error() string { return fmt.Sprintf("myErr(%d)", m.code) }

// TestErrorsUnwrap verifies that errors.Is and errors.As traverse an *Errors
// aggregate (issue #73: callers must be able to detect sentinel errors such
// as sql.ErrNoRows, sql.ErrTxDone, context.Canceled when an aggregate is
// returned).
func TestErrorsUnwrap(t *testing.T) {
	t.Run("Unwrap_returns_items", func(t *testing.T) {
		a := errors.New("a")
		b := errors.New("b")
		e := &Errors{items: []error{a, b}}

		require.Equal(t, []error{a, b}, e.Unwrap())
	})

	t.Run("Unwrap_nil_on_empty", func(t *testing.T) {
		e := &Errors{}
		require.Nil(t, e.Unwrap())
	})

	t.Run("Unwrap_nil_on_nil_receiver", func(t *testing.T) {
		var e *Errors
		require.Nil(t, e.Unwrap())
	})

	t.Run("errors_Is_finds_sentinel_via_Unwrap", func(t *testing.T) {
		other := errors.New("other failure")
		e := &Errors{items: []error{other, sql.ErrNoRows}}

		require.True(t, errors.Is(e, sql.ErrNoRows),
			"errors.Is must reach sql.ErrNoRows through *Errors.Unwrap")
	})

	t.Run("errors_Is_returns_false_when_no_match", func(t *testing.T) {
		e := &Errors{items: []error{errors.New("a"), errors.New("b")}}

		require.False(t, errors.Is(e, sql.ErrNoRows))
	})

	t.Run("errors_Is_finds_wrapped_sentinel", func(t *testing.T) {
		// caller wrapped sql.ErrNoRows before aggregation; the chain must
		// still unwrap through fmt.Errorf("%w", ...).
		wrapped := fmt.Errorf("query failed: %w", sql.ErrNoRows)
		e := &Errors{items: []error{wrapped}}

		require.True(t, errors.Is(e, sql.ErrNoRows),
			"errors.Is must follow fmt.Errorf %w chains inside an *Errors aggregate")
	})

	t.Run("errors_Is_handles_recursion", func(t *testing.T) {
		// an *Errors nested inside another *Errors must also be reachable.
		inner := &Errors{items: []error{sql.ErrNoRows}}
		outer := &Errors{items: []error{errors.New("skip"), inner}}

		require.True(t, errors.Is(outer, sql.ErrNoRows),
			"errors.Is must recurse through nested *Errors aggregates")
	})

	t.Run("errors_As_extracts_target_type", func(t *testing.T) {
		want := &myErr{code: 7}
		e := &Errors{items: []error{errors.New("other"), want}}

		var got *myErr
		require.True(t, errors.As(e, &got), "errors.As must reach *myErr through *Errors")
		require.Same(t, want, got)
	})

	t.Run("Error_returns_string_form", func(t *testing.T) {
		e := &Errors{items: []error{errors.New("x"), errors.New("y")}}
		// fmt.Sprint of a slice yields "[x y]"; we don't pin the exact
		// whitespace but we must include both error strings.
		s := e.Error()
		require.Contains(t, s, "x")
		require.Contains(t, s, "y")
	})

	t.Run("Is_method_directly", func(t *testing.T) {
		e := &Errors{items: []error{errors.New("a"), sql.ErrNoRows}}

		require.True(t, e.Is(sql.ErrNoRows))
		require.False(t, e.Is(sql.ErrTxDone))
	})

	t.Run("Is_method_nil_receiver", func(t *testing.T) {
		var e *Errors
		require.True(t, e.Is(nil))
		require.False(t, e.Is(errors.New("anything")))
	})
}
