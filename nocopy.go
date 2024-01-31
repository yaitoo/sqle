package sqle

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
