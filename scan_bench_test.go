package sqle

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// benchScanDB opens an in-memory sqlite populated with `rows` rows × `cols`
// columns. Each row has distinct values so the work is real (no caching
// shortcut at the driver layer). cols must be ≥ 1.
func benchScanDB(b *testing.B, rows, cols int) *sql.DB {
	if cols < 1 {
		cols = 1
	}
	d, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}

	defs := make([]byte, 0, 256)
	defs = append(defs, "CREATE TABLE bench (id INTEGER PRIMARY KEY"...)
	for i := 1; i <= cols; i++ {
		defs = append(defs, ", c"...)
		defs = strconvAppendInt(defs, i)
		defs = append(defs, " INTEGER"...)
	}
	defs = append(defs, ')')
	if _, err := d.Exec(string(defs)); err != nil {
		b.Fatal(err)
	}

	args := make([]byte, 0, 64)
	for r := 0; r < rows; r++ {
		args = args[:0]
		args = append(args, "INSERT INTO bench VALUES ("...)
		args = strconvAppendInt(args, r)
		for c := 1; c <= cols; c++ {
			args = append(args, ',')
			args = strconvAppendInt(args, r*cols+c)
		}
		args = append(args, ')')
		if _, err := d.Exec(string(args)); err != nil {
			b.Fatal(err)
		}
	}
	return d
}

func strconvAppendInt(dst []byte, x int) []byte {
	return strconvItoa(dst, x)
}

func strconvItoa(dst []byte, x int) []byte {
	if x == 0 {
		return append(dst, '0')
	}
	neg := false
	if x < 0 {
		neg = true
		x = -x
	}
	var buf [20]byte
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return append(dst, buf[i:]...)
}

// resetColumnsCache forces a fresh column-resolution on the next Bind so
// per-benchmark iterators don't share cached column lists.
func resetColumnsCache() {
	columns = newLRUCache[string, []string](4096)
}

// benchRow is the struct shape used by BenchmarkScanToStructList.
type benchRow struct {
	ID int64 `db:"id"`
	C1 int64 `db:"c1"`
	C3 int64 `db:"c3"`
	C5 int64 `db:"c5"`
	C7 int64 `db:"c7"`
}

// benchBinder is the binder-implementing shape used by
// BenchmarkScanToBinderList. Its pointer-receiver method satisfies the
// public Binder interface; for []benchBinder rows.go's reflect.New(elem)
// yields a *benchBinder that scanToBinderList dispatches on.
type benchBinder struct {
	ID int
	C1 int
	C2 int
	C3 int
}

func (b *benchBinder) Bind(_ reflect.Value, cols []string) []any {
	out := make([]any, len(cols))
	for i, n := range cols {
		switch n {
		case "id":
			out[i] = &b.ID
		case "c1":
			out[i] = &b.C1
		case "c2":
			out[i] = &b.C2
		case "c3":
			out[i] = &b.C3
		}
	}
	return out
}

func repeatCols(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, n*5)
	for i := 1; i <= n; i++ {
		out = append(out, ", c"...)
		out = strconvAppendInt(out, i)
	}
	return string(out)
}

// runScanBench executes fn b.N times, opening a fresh sqlite + query per
// iteration so each Bind sees its own cursor (Rows is closed after a
// successful Bind, so re-using a single cursor across iterations would
// surface "Rows are closed" on the second call).
func runScanBench(b *testing.B, cols int, fn func(q *Rows)) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d := benchScanDB(b, 64, cols)
		db := Open(d)
		stmt := New("SELECT id" + repeatCols(cols) + " FROM bench")
		q, err := db.QueryBuilder(context.Background(), stmt)
		if err != nil {
			d.Close()
			b.Fatal(err)
		}
		resetColumnsCache()
		b.StartTimer()

		fn(q)

		b.StopTimer()
		q.Close()
		d.Close()
		b.StartTimer()
	}
}

// BenchmarkScanToMapList measures allocations when binding
// []map[string]int64 from a multi-row result set.
func BenchmarkScanToMapList(b *testing.B) {
	runScanBench(b, 20, func(q *Rows) {
		var out []map[string]int64
		if err := q.Bind(&out); err != nil {
			b.Fatal(err)
		}
	})
}

// BenchmarkScanToListPrimitive measures allocations when binding
// [][]int64 — the primitive branch of scanToList.
func BenchmarkScanToListPrimitive(b *testing.B) {
	runScanBench(b, 20, func(q *Rows) {
		var out [][]int64
		if err := q.Bind(&out); err != nil {
			b.Fatal(err)
		}
	})
}

// BenchmarkScanToStructList measures allocations when binding a slice of
// structs into a multi-row result set (structBinder path).
func BenchmarkScanToStructList(b *testing.B) {
	runScanBench(b, 20, func(q *Rows) {
		var out []benchRow
		if err := q.Bind(&out); err != nil {
			b.Fatal(err)
		}
	})
}

// BenchmarkScanToBinderList measures allocations when binding a slice of
// types that implement the Binder interface (scanToBinderList path).
func BenchmarkScanToBinderList(b *testing.B) {
	runScanBench(b, 3, func(q *Rows) {
		var out []benchBinder
		if err := q.Bind(&out); err != nil {
			b.Fatal(err)
		}
	})
}