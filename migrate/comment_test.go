package migrate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripSQLComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// --- baseline ---
		{
			name: "no_comments",
			in:   "CREATE TABLE foo (id INT);",
			want: "CREATE TABLE foo (id INT);",
		},
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "only_whitespace",
			in:   "  \n\t",
			want: "  \n\t",
		},

		// --- `--` line comment at token boundaries (must be stripped) ---
		{
			name: "line_comment_at_start_of_file",
			in:   "-- drop foo; keep bar;\nCREATE TABLE foo (id INT);",
			want: "\nCREATE TABLE foo (id INT);",
		},
		{
			name: "line_comment_after_newline",
			in:   "SELECT 5;\n-- a note;\nSELECT 6;",
			want: "SELECT 5;\n\nSELECT 6;",
		},
		{
			name: "line_comment_after_space",
			in:   "SELECT 5 -- a note;\n; SELECT 6;",
			want: "SELECT 5 \n; SELECT 6;",
		},
		{
			name: "line_comment_after_tab",
			in:   "SELECT 5\t-- a note\n;",
			want: "SELECT 5\t\n;",
		},
		{
			name: "line_comment_after_carriage_return",
			in:   "SELECT 5\r-- a note\n;",
			want: "SELECT 5\r\n;",
		},
		{
			name: "line_comment_after_semicolon",
			in:   "SELECT 5;-- a note",
			want: "SELECT 5;",
		},
		{
			name: "line_comment_after_open_paren",
			in:   "VALUES (-- a note\n1)",
			want: "VALUES (\n1)",
		},
		{
			name: "line_comment_after_comma",
			in:   "VALUES (1, -- a note\n2)",
			want: "VALUES (1, \n2)",
		},
		{
			name: "line_comment_after_close_paren",
			in:   "(1) -- a note",
			want: "(1) ",
		},
		{
			name: "line_comment_after_single_quote",
			in:   "SELECT 'a'-- a note\nFROM t;",
			want: "SELECT 'a'\nFROM t;",
		},
		{
			name: "line_comment_after_double_quote",
			in:   `SELECT "a"-- a note` + "\nFROM t;",
			want: `SELECT "a"` + "\nFROM t;",
		},
		{
			name: "line_comment_after_backtick",
			in:   "SELECT `a`-- a note\nFROM t;",
			want: "SELECT `a`\nFROM t;",
		},
		{
			name: "line_comment_at_eof_no_newline",
			in:   "SELECT 5;-- trailing",
			want: "SELECT 5;",
		},

		// --- `--` at non-boundary positions (must NOT be stripped) ---
		{
			name: "dash_dash_after_digit_preserved",
			in:   "5--3",
			want: "5--3",
		},
		{
			name: "dash_dash_after_letter_preserved",
			in:   "a--b",
			want: "a--b",
		},
		{
			name: "dash_dash_after_keyword_preserved",
			in:   "SELECT--comment\nFROM t;",
			want: "SELECT--comment\nFROM t;",
		},
		{
			name: "dash_dash_after_dot_preserved",
			in:   "1.5--3",
			want: "1.5--3",
		},
		{
			name: "dash_dash_after_plus_preserved",
			in:   "5+-3",
			want: "5+-3",
		},
		{
			name: "dash_dash_after_equals_preserved",
			in:   "a=--b",
			want: "a=--b",
		},
		{
			name: "dash_dash_after_another_dash_preserved",
			in:   "5---3",
			want: "5---3",
		},

		// --- `/* … */` block comment (always a comment, no boundary check) ---
		{
			name: "block_comment_at_start_of_file",
			in:   "/* drop foo; keep bar; */\nCREATE TABLE foo (id INT);",
			want: "\nCREATE TABLE foo (id INT);",
		},
		{
			name: "block_comment_after_digit",
			in:   "5/*3*/x",
			want: "5x",
		},
		{
			name: "block_comment_after_letter",
			in:   "a/*b*/x",
			want: "ax",
		},
		{
			name: "block_comment_spans_lines",
			in:   "/* line one;\n   line two;\n*/\nCREATE TABLE foo (id INT);",
			want: "\nCREATE TABLE foo (id INT);",
		},
		{
			name: "empty_block_comment",
			in:   "SELECT 1 /**/ ;",
			want: "SELECT 1  ;",
		},
		{
			name: "block_comment_with_star_inside",
			in:   "/* a * b */ x",
			want: " x",
		},
		{
			name: "line_comment_inside_block_comment",
			in:   "/* outer -- with semicolon; still inside */ CREATE TABLE foo (id INT);",
			want: " CREATE TABLE foo (id INT);",
		},
		{
			name: "nested_lookalike_block_inside_block",
			in:   "/* outer /* inner */ still inside */ x",
			want: " still inside */ x",
		},
		{
			name: "unterminated_block_comment",
			in:   "/* still inside; never closes\nCREATE TABLE foo (id INT);",
			want: "",
		},

		// --- single-quoted strings ---
		{
			name: "single_quoted_string_with_dash_dash",
			in:   "SELECT 'a--b' FROM t;",
			want: "SELECT 'a--b' FROM t;",
		},
		{
			name: "single_quoted_string_with_block_comment",
			in:   "SELECT 'a/*b' FROM t;",
			want: "SELECT 'a/*b' FROM t;",
		},
		{
			name: "single_quoted_string_with_semicolon",
			in:   "SELECT 'a;b' FROM t;",
			want: "SELECT 'a;b' FROM t;",
		},
		{
			name: "single_quoted_string_with_escaped_quote",
			in:   "SELECT 'it''s; ok' FROM t;",
			want: "SELECT 'it''s; ok' FROM t;",
		},
		{
			name: "single_quoted_string_with_newline",
			in:   "SELECT 'a\nb' FROM t;",
			want: "SELECT 'a\nb' FROM t;",
		},
		{
			name: "dash_dash_inside_then_line_comment_outside",
			in:   "SELECT 'a--b' -- real comment\nFROM t;",
			want: "SELECT 'a--b' \nFROM t;",
		},
		{
			name: "unterminated_single_quoted_string",
			in:   "SELECT 'never closes",
			want: "SELECT 'never closes",
		},

		// --- double-quoted strings (MySQL with ANSI_QUOTES off, or ANSI SQL) ---
		{
			name: "double_quoted_string_with_dash_dash",
			in:   `SELECT "a--b" FROM t;`,
			want: `SELECT "a--b" FROM t;`,
		},
		{
			name: "double_quoted_string_with_block_comment",
			in:   `SELECT "a/*b" FROM t;`,
			want: `SELECT "a/*b" FROM t;`,
		},
		{
			name: "double_quoted_string_with_semicolon",
			in:   `SELECT "a;b" FROM t;`,
			want: `SELECT "a;b" FROM t;`,
		},
		{
			name: "double_quoted_string_with_backslash_escape",
			in:   `SELECT "a\"b" FROM t;`,
			want: `SELECT "a\"b" FROM t;`,
		},
		{
			name: "double_quoted_string_with_doubled_quote_escape",
			in:   `SELECT "a""b" FROM t;`,
			want: `SELECT "a""b" FROM t;`,
		},
		{
			name: "double_quoted_string_with_only_doubled_quote",
			in:   `SELECT "" FROM t;`,
			want: `SELECT "" FROM t;`,
		},
		{
			name: "backtick_inside_double_quoted_preserved",
			// backtick inside a "..." literal must not close the
			// double-quoted string.
			in:   "SELECT \"a`b\" FROM t;",
			want: "SELECT \"a`b\" FROM t;",
		},
		{
			name: "double_quoted_containing_backtick_and_semicolon",
			// a backtick and a semicolon inside "..." must not end
			// the string nor be misread as a statement separator.
			in:   "SELECT \"a`; SELECT 2\" FROM t;",
			want: "SELECT \"a`; SELECT 2\" FROM t;",
		},
		{
			name: "unterminated_double_quoted_string",
			in:   `SELECT "never closes`,
			want: `SELECT "never closes`,
		},

		// --- backtick strings (MySQL identifiers) ---
		{
			name: "backtick_string_with_dash_dash",
			in:   "SELECT `a--b` FROM t;",
			want: "SELECT `a--b` FROM t;",
		},
		{
			name: "backtick_string_with_block_comment",
			in:   "SELECT `a/*b` FROM t;",
			want: "SELECT `a/*b` FROM t;",
		},
		{
			name: "backtick_string_with_semicolon",
			in:   "SELECT `a;b` FROM t;",
			want: "SELECT `a;b` FROM t;",
		},
		{
			name: "backtick_string_with_backslash_escape",
			in:   "SELECT `a\\`b` FROM t;",
			want: "SELECT `a\\`b` FROM t;",
		},
		{
			name: "backtick_string_with_doubled_backtick_escape",
			// MySQL lets you escape a backtick inside a backtick
			// identifier by doubling it.
			in:   "SELECT `a``b` FROM t;",
			want: "SELECT `a``b` FROM t;",
		},
		{
			name: "unterminated_backtick_string",
			in:   "SELECT `never closes",
			want: "SELECT `never closes",
		},
		{
			name: "double_quote_inside_backtick_preserved",
			// a " inside a `...` literal must not close the
			// backtick string, and any subsequent text must remain
			// inside the literal.
			in:   "SELECT `a\"b` FROM t;",
			want: "SELECT `a\"b` FROM t;",
		},
		{
			name: "backtick_string_containing_double_quote_and_semicolon",
			in:   "SELECT `a\"; SELECT 2` FROM t;",
			want: "SELECT `a\"; SELECT 2` FROM t;",
		},
		{
			name: "real_comment_after_backtick_with_double_quote_inside",
			// a real `--` comment after a backtick string that
			// itself contains a `"` must be stripped. The previous
			// (buggy) implementation left it untouched because it
			// thought the `"` inside the literal had closed it.
			in:   "SELECT `a\"b` -- real comment\nFROM t;",
			want: "SELECT `a\"b` \nFROM t;",
		},

		// --- combined end-to-end scripts ---
		{
			name: "multiple_statements_with_mixed_comments",
			in: "CREATE TABLE a (id INT); -- done with a;\n" +
				"CREATE TABLE b (id INT); /* done with b; */\n" +
				"CREATE TABLE c (id INT);",
			want: "CREATE TABLE a (id INT); \n" +
				"CREATE TABLE b (id INT); \n" +
				"CREATE TABLE c (id INT);",
		},
		{
			name: "semicolon_in_string_then_real_semicolon",
			in:   "INSERT INTO foo VALUES ('a;b'); INSERT INTO bar VALUES (1);",
			want: "INSERT INTO foo VALUES ('a;b'); INSERT INTO bar VALUES (1);",
		},
		{
			name: "comment_with_quote_inside_does_not_open_string",
			in:   "SELECT 1; -- it's a comment\nSELECT 2;",
			want: "SELECT 1; \nSELECT 2;",
		},
		{
			name: "block_comment_with_single_quote_inside",
			in:   "/* it's a comment; with 'quote' */ SELECT 1;",
			want: " SELECT 1;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stripSQLComments(tt.in))
		})
	}
}

// TestStripSQLCommentsSemicolonSplit documents the bug we are fixing:
// before stripSQLComments was added, splitting a script on ';' would
// treat ';' inside a comment as a statement terminator and the
// non-SQL fragment from the comment would then be executed.
func TestStripSQLCommentsSemicolonSplit(t *testing.T) {
	script := "-- TODO: drop foo; keep bar;\nCREATE TABLE foo (id INT);"

	naive := strings.Split(script, ";")
	// Naive split yields a comment-only fragment as a "statement".
	require.Contains(t, naive[0], "-- TODO: drop foo")

	clean := strings.Split(stripSQLComments(script), ";")
	// After stripping, the only non-empty statement is the CREATE TABLE.
	var nonEmpty []string
	for _, s := range clean {
		if strings.TrimSpace(s) != "" {
			nonEmpty = append(nonEmpty, s)
		}
	}
	require.Len(t, nonEmpty, 1)
	require.Contains(t, nonEmpty[0], "CREATE TABLE foo")
}

// TestStripSQLCommentsLegitimateContentPreserved asserts that the
// helper does not silently delete content that is not a comment:
// numbers, identifiers, dotted decimals, and string contents must
// round-trip unchanged.
func TestStripSQLCommentsLegitimateContentPreserved(t *testing.T) {
	inputs := []string{
		"SELECT 5--3",                    // -- after digit, not a comment
		"SELECT a--b",                    // -- after letter, not a comment
		"SELECT 1.5--3",                  // -- after digit in decimal
		"SELECT 5+-3",                    // -- after operator
		"SELECT 5---3",                   // -- after another dash
		"SELECT `a--b` FROM t",           // -- inside backtick
		`SELECT "a--b" FROM t`,           // -- inside double-quoted
		"SELECT 'a--b' FROM t",           // -- inside single-quoted
		"SELECT 'a/*b' FROM t",           // /* inside single-quoted
		"INSERT INTO foo VALUES ('a;b')", // ; inside single-quoted
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, in, stripSQLComments(in))
		})
	}
}
