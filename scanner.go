package sqle

type Scanner interface {
	Scan(dest ...any) error
	Close() error
}
