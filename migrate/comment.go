package migrate

import "strings"

// stripSQLComments removes SQL single-line (`-- …`) and multi-line
// (`/* … */`) comments from a script so the caller can safely split
// it on `;` without treating a semicolon inside a comment as a
// statement terminator.
//
// String literals are recognised so that `--` or `/*` inside a
// literal is left alone:
//   - '…' single-quoted strings, with the standard '' escape
//   - "…" double-quoted strings (e.g. MySQL with ANSI_QUOTES off)
//   - `…` backtick-quoted strings (MySQL identifiers), with \ escapes
//
// `--` is only treated as the start of a comment when it appears at a
// real token boundary — the start of the script, after whitespace,
// or after a delimiter such as `; ( ) , ' " \`` — so that constructs
// like `5--3` are not silently mutated. `/*` has no such ambiguity
// in SQL and is always treated as the start of a block comment.
func stripSQLComments(scripts string) string {
	var b strings.Builder
	b.Grow(len(scripts))

	n := len(scripts)

	inLine := false    // inside a -- ... \n comment
	inBlock := false   // inside a /* ... */ comment
	inString := false  // inside a '...' string literal
	inQString := false // inside a "..." or `...` quoted identifier/string

	for i := 0; i < n; i++ {
		c := scripts[i]

		if inLine {
			if c == '\n' {
				inLine = false
				b.WriteByte(c)
			}
			continue
		}

		if inBlock {
			if c == '*' && i+1 < n && scripts[i+1] == '/' {
				inBlock = false
				i++ // skip the closing '/'
			}
			continue
		}

		if inString {
			b.WriteByte(c)
			if c == '\'' {
				if i+1 < n && scripts[i+1] == '\'' {
					// SQL '' escape: keep both quotes inside the string.
					b.WriteByte(scripts[i+1])
					i++
				} else {
					inString = false
				}
			}
			continue
		}

		if inQString {
			b.WriteByte(c)
			if c == '\\' && i+1 < n {
				// Backslash escape for " or ` (MySQL).
				b.WriteByte(scripts[i+1])
				i++
				continue
			}
			if c == '"' || c == '`' {
				inQString = false
			}
			continue
		}

		if c == '\'' {
			inString = true
			b.WriteByte(c)
			continue
		}

		if c == '"' || c == '`' {
			inQString = true
			b.WriteByte(c)
			continue
		}

		if c == '-' && i+1 < n && scripts[i+1] == '-' && isLineCommentStart(scripts, i) {
			inLine = true
			i++ // skip the second '-'
			continue
		}

		if c == '/' && i+1 < n && scripts[i+1] == '*' {
			inBlock = true
			i++ // skip the '*'
			continue
		}

		b.WriteByte(c)
	}

	return b.String()
}

// isLineCommentStart reports whether the '-' at position i in scripts
// is at a real token boundary and therefore starts an SQL line
// comment.
func isLineCommentStart(scripts string, i int) bool {
	if i == 0 {
		return true
	}
	switch scripts[i-1] {
	case ' ', '\t', '\n', '\r', ';', '(', ',', ')', '\'', '"', '`':
		return true
	}
	return false
}
