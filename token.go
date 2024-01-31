package sqle

type TokenType uint

const (
	TextToken  TokenType = 0
	InputToken TokenType = 1
	ParamToken TokenType = 2
)

type Token interface {
	Type() TokenType
	String() string
}

type Text string

func (t Text) Type() TokenType {
	return TextToken
}
func (t Text) String() string {
	return string(t)
}

type Input string

func (t Input) Type() TokenType {
	return InputToken
}
func (t Input) String() string {
	return string(t)
}

type Param string

func (t Param) Type() TokenType {
	return ParamToken
}
func (t Param) String() string {
	return string(t)
}
