// Package sqle provides a SQLBuilder for constructing SQL statements in a programmatic way.
// It allows you to build SELECT, INSERT, UPDATE, and DELETE statements with ease.
package sqle

import (
	"errors"
	"sort"
	"strings"

	"github.com/yaitoo/sqle/shardid"
)

var (
	// ErrInvalidParamVariable is an error that is returned when an invalid parameter variable is encountered.
	ErrInvalidParamVariable = errors.New("sqle: invalid param variable")

	// ErrInvalidIdentifier is returned when a table name (Update/Insert/
	// Select/Delete) or a Select column name is empty or contains characters
	// that are not permitted in a SQL identifier. It is surfaced from Build.
	//
	// Identifiers are validated against a strict shape: a leading letter or
	// underscore followed by letters, digits and underscores, with optional
	// <name> placeholders (used by input substitution, e.g. orders<rotate>)
	// and an optional dotted schema/table form (schema.table). This prevents
	// payloads like `id; DROP TABLE users` from reaching the wire protocol
	// even though the surrounding quoting already prevents their execution.
	//
	// Note: column names passed via UpdateBuilder.Set/SetMap and
	// InsertBuilder.Set/SetMap/End are not validated here; callers must
	// restrict those via WithAllow or other whitelists.
	ErrInvalidIdentifier = errors.New("sqle: invalid identifier")

	// DefaultSQLQuote is the default character used to escape column names in UPDATE and INSERT statements.
	DefaultSQLQuote = "`"

	// DefaultSQLParameterize is the default function used to parameterize values in SQL statements.
	DefaultSQLParameterize = func(name string, index int) string {
		return "?"
	}
)

// Builder is a SQL query builder that allows you to construct SQL statements.
type Builder struct {
	stmt       strings.Builder
	inputs     map[string]string
	params     map[string]any
	shouldSkip bool
	err        error // deferred error from validation; surfaced by Build

	Quote        string // escape column name in UPDATE and INSERT
	Parameterize func(name string, index int) string
}

// New creates a new instance of the Builder with the given initial command(s).
func New(cmd ...string) *Builder {
	b := &Builder{
		inputs:       make(map[string]string),
		params:       make(map[string]any),
		Quote:        DefaultSQLQuote,
		Parameterize: DefaultSQLParameterize,
	}

	for i, it := range cmd {
		if i > 0 {
			b.stmt.WriteString(" ")
		}
		b.stmt.WriteString(it)
	}

	return b
}

// Input sets the value of an input variable in the Builder.
func (b *Builder) Input(name, value string) *Builder {
	b.inputs[name] = value
	return b
}

// Inputs sets multiple input variables in the Builder.
func (b *Builder) Inputs(v map[string]string) *Builder {
	for n, v := range v {
		b.Input(n, v)
	}

	return b
}

// Param sets the value of a parameter variable in the Builder.
func (b *Builder) Param(name string, value any) *Builder {
	b.params[name] = value
	return b
}

// Params sets multiple parameter variables in the Builder.
func (b *Builder) Params(v map[string]any) *Builder {
	for n, v := range v {
		b.Param(n, v)
	}

	return b
}

// If sets a condition that determines whether the subsequent SQL command should be executed.
// If the predicate is false, the command is skipped.
func (b *Builder) If(predicate bool) *Builder {
	b.shouldSkip = !predicate
	return b
}

// SQL appends the given SQL command to the Builder's statement.
// If the Builder's shouldSkip flag is set, the command is skipped.
// If a prior validation already recorded an error, the command is dropped so
// the attack string never lands in the buffer (Build short-circuits on the
// stored error and returns the sentinel).
func (b *Builder) SQL(cmd string) *Builder {
	if b.err != nil {
		return b
	}

	if b.shouldSkip {
		b.shouldSkip = false
		return b
	}

	if cmd != "" {
		b.stmt.WriteString(cmd)
	}
	return b
}

// String returns the SQL statement constructed by the Builder.
func (b *Builder) String() string {
	return b.stmt.String()
}

// Build constructs the final SQL statement and returns it along with the parameter values.
func (b *Builder) Build() (string, []any, error) {
	if b.err != nil {
		return "", nil, b.err
	}

	tz := Tokenize(b.stmt.String())

	var params []any
	var sb strings.Builder
	i := 1

	for _, t := range tz.Tokens {
		switch t.Type() {
		case TextToken:
			sb.WriteString(t.String())
		case InputToken:
			n := t.String()
			v, ok := b.inputs[n]
			if ok {
				sb.WriteString(v)
			}

		case ParamToken:
			n := t.String()
			v, ok := b.params[n]
			if !ok {
				return "", nil, ErrInvalidParamVariable
			}

			sb.WriteString(b.Parameterize(n, i))
			i++
			params = append(params, v)
		}

	}

	return sb.String(), params, nil

}

// clone returns a copy of b whose stmt buffer and inputs/params maps are
// independent of b: stmt is rebuilt from the current SQL string so further
// writes on the clone do not touch the original, and inputs/params are fresh
// maps populated with the current key/value pairs.
//
// clone is used by MapR (First/Count/Query/QueryLimit) to inject <rotate> and
// the LIMIT clause without mutating the caller's *Builder.
//
// Note: the values stored in inputs and params are shared with the original
// builder (not deep-copied). The shallow copy is safe for two reasons:
//
//  1. inputs values are Go strings, which are immutable.
//  2. The rest of this package never mutates a stored value in place —
//     Builder.Input / Param / Inputs / Params only assign keys, and
//     Builder.Build only reads values. As long as that invariant holds,
//     a future caller mutating a stored value (e.g. appending to a slice
//     or writing through a pointer held in params) will leak that mutation
//     across both builders; this method does not protect against that.
func (b *Builder) clone() *Builder {
	s := b.stmt.String()
	nb := &Builder{
		stmt:         strings.Builder{},
		inputs:       make(map[string]string, len(b.inputs)),
		params:       make(map[string]any, len(b.params)),
		shouldSkip:   b.shouldSkip,
		err:          b.err,
		Quote:        b.Quote,
		Parameterize: b.Parameterize,
	}
	nb.stmt.Grow(len(s))
	nb.stmt.WriteString(s)
	for n, v := range b.inputs {
		nb.inputs[n] = v
	}
	for n, v := range b.params {
		nb.params[n] = v
	}
	return nb
}

// isPlainIdentifier reports whether s is a non-empty SQL identifier segment:
// a leading letter or underscore followed by letters, digits and underscores.
func isPlainIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 {
			if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && c != '_' {
				return false
			}
			continue
		}
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_' {
			return false
		}
	}
	return true
}

// validateIdentifierSegment reports whether a single dotted segment of an
// identifier is well-formed: a concatenation of plain identifiers (matching
// isPlainIdentifier) and <name> placeholders (e.g. `<rotate>`) used for input
// substitution. A leading `<`, trailing `>` mismatch, or any character outside
// [A-Za-z0-9_<>] is rejected.
func validateIdentifierSegment(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	for i < len(s) {
		if s[i] == '<' {
			end := strings.IndexByte(s[i:], '>')
			if end <= 1 {
				// `<` with no matching `>`, or `<>` / `<>` empty name.
				return false
			}
			if !isPlainIdentifier(s[i+1 : i+end]) {
				return false
			}
			i += end + 1
			continue
		}
		// Plain identifier until the next `<` or end of segment.
		next := strings.IndexByte(s[i:], '<')
		if next == -1 {
			return isPlainIdentifier(s[i:])
		}
		if next == 0 {
			return false
		}
		if !isPlainIdentifier(s[i : i+next]) {
			return false
		}
		i += next
	}
	return true
}

// validateIdentifier reports whether s is a well-formed SQL identifier. A
// valid identifier is a non-empty sequence of identifier characters
// ([A-Za-z_][A-Za-z0-9_]*) and optional <name> placeholders, optionally
// qualified by a single dotted schema/table form (schema.table). Anything
// else — empty string, embedded backticks, semicolons, spaces, SQL
// comments — is rejected.
func validateIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, seg := range strings.Split(s, ".") {
		if !validateIdentifierSegment(seg) {
			return false
		}
	}
	return true
}

// markInvalidIdentifier records ErrInvalidIdentifier on the Builder so it
// surfaces from Build, unless an error is already recorded. The first error
// wins so a chain of bad inputs reports the same sentinel to callers.
func (b *Builder) markInvalidIdentifier() {
	if b.err == nil {
		b.err = ErrInvalidIdentifier
	}
}

// containsDangerousColumnChars reports whether c contains any token that
// would let a column expression break out of the SQL context: the surrounding
// quote, statement terminators, line breaks, or the SQL comment sequences
// "--", "/*", and "*/". Single arithmetic bytes ("-", "/", "*") are allowed
// because they appear in legitimate expressions like `price * qty`.
func containsDangerousColumnChars(c string) bool {
	if strings.ContainsAny(c, "`;\n\r") {
		return true
	}
	if strings.Contains(c, "--") || strings.Contains(c, "/*") || strings.Contains(c, "*/") {
		return true
	}
	return false
}

// quoteColumn escapes the given column name using the Builder's Quote character.
// A column that contains '(' or space is treated as an expression (e.g.
// `count(id)`, `id + 1`) and passed through unchanged; the caller is
// responsible for the expression's syntax. Plain identifiers are validated and
// quoted. Any column — expression or identifier — that is empty, contains a
// breakout character, or fails identifier validation records
// ErrInvalidIdentifier on the Builder (surfaced from Build) and returns "" so
// the buffer stays clean of the attack string.
func (b *Builder) quoteColumn(c string) string {
	if c == "" || containsDangerousColumnChars(c) {
		b.markInvalidIdentifier()
		return ""
	}

	if strings.ContainsAny(c, "(") || strings.ContainsAny(c, " ") {
		return c
	}

	if !validateIdentifier(c) {
		b.markInvalidIdentifier()
		return ""
	}

	return b.Quote + c + b.Quote
}

// Update starts a new UpdateBuilder and sets the table to update.
// Returns the new UpdateBuilder.
func (b *Builder) Update(table string) *UpdateBuilder {
	if !validateIdentifier(table) {
		b.markInvalidIdentifier()
	} else {
		b.SQL("UPDATE ").SQL(b.Quote).SQL(table).SQL(b.Quote).SQL(" SET ")
	}
	return &UpdateBuilder{
		Builder: b,
	}
}

// Insert starts a new InsertBuilder and sets the table to insert into.
// Returns the new InsertBuilder.
func (b *Builder) Insert(table string) *InsertBuilder {
	if !validateIdentifier(table) {
		b.markInvalidIdentifier()
	}
	return &InsertBuilder{
		b:      b,
		table:  table,
		values: make(map[string]any),
	}
}

// Select adds a SELECT statement to the current query builder.
// If no columns are specified, it selects all columns using "*".
// Returns the current query builder.
//
// Column names passed by the caller are trusted expressions (anything
// containing `(` or space) or validated plain identifiers; see quoteColumn.
// When the table name contains characters outside the strict identifier
// shape (for example `users; DROP TABLE users`), ErrInvalidIdentifier is
// recorded on the Builder and surfaced from Build.
func (b *Builder) Select(table string, columns ...string) *Builder {
	b.SQL("SELECT")

	if columns == nil {
		b.SQL(" *")
	} else {
		for i, col := range columns {
			if i == 0 {
				b.SQL(" ").SQL(b.quoteColumn(col))
			} else {
				b.SQL(" ,").SQL(b.quoteColumn(col))
			}
		}
	}

	if !validateIdentifier(table) {
		b.markInvalidIdentifier()
	} else {
		b.SQL(" FROM ").SQL(b.Quote).SQL(table).SQL(b.Quote)
	}

	return b
}

// Delete adds a DELETE statement to the current query builder.
// Returns the current query builder.
func (b *Builder) Delete(table string) *Builder {
	if !validateIdentifier(table) {
		b.markInvalidIdentifier()
	} else {
		b.SQL("DELETE FROM ").SQL(b.Quote).SQL(table).SQL(b.Quote)
	}

	return b
}

// On sets the "rotate" input variable to the given shard ID's rotate name.
// Returns the current query builder.
func (b *Builder) On(id shardid.ID) *Builder {
	return b.Input("rotate", id.RotateName())
}

// sortColumns sorts the columns in the given map and returns them as a pre-sorted columns slice.
// It helps PrepareStmt works with sql statement as less as possible.
// It also allows customization of column names using BuilderOptions.
func sortColumns(m map[string]any, opts ...BuilderOption) []string {
	bo := &BuilderOptions{}
	for _, opt := range opts {
		opt(bo)
	}

	hasCustomizedColumns := len(bo.Columns) > 0

	for n, v := range m {
		name := n

		if bo.ToName != nil {
			name = bo.ToName(name)
			if name != n {
				m[name] = v
			}
		}

		if !hasCustomizedColumns {
			bo.Columns = append(bo.Columns, name)
		}
	}

	if !hasCustomizedColumns {
		sort.Strings(bo.Columns)
	}

	return bo.Columns
}
