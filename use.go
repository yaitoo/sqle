package sqle

import "strconv"

func UsePostgres(b *Builder) {
	b.Quote = "`"
	b.Parameterize = func(name string, index int) string {
		return "$" + strconv.Itoa(index)
	}
}

func UseMySQL(b *Builder) {
	b.Quote = "`"
	b.Parameterize = func(name string, index int) string {
		return "?"
	}
}

// isOracleBindName reports whether name is a valid Oracle named bind
// variable: a leading ASCII letter followed by ASCII letters, digits, or
// underscores. Names that fail this check (hyphens, dots, leading digits,
// leading underscores, etc.) are emitted as positional placeholders by
// UseOracle because Oracle would otherwise parse `:user-id` as
// subtraction and `:2foo` as a numeric literal.
func isOracleBindName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if i == 0 {
			if !isASCIILetter(c) {
				return false
			}
			continue
		}
		if !isASCIILetter(c) && !isASCIIDigit(c) && c != '_' {
			return false
		}
	}
	return true
}

func UseOracle(b *Builder) {
	b.Quote = "`"
	b.Parameterize = func(name string, index int) string {
		if isOracleBindName(name) {
			return ":" + name
		}
		// Fall back to a positional bind when the parameter name contains
		// characters Oracle cannot parse as a single bind variable (e.g.
		// hyphens or dots from tokenRegexp-widened placeholders like
		// {user-id} or {module.field}).
		return ":p" + strconv.Itoa(index)
	}
}
