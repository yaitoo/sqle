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

func UseOracle(b *Builder) {
	b.Quote = "`"
	b.Parameterize = func(name string, index int) string {
		return ":" + name
	}
}
