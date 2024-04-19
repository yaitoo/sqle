package sqle

// TokenType represents the type of a token.
type TokenType uint

const (
	TextToken  TokenType = 0
	InputToken TokenType = 1
	ParamToken TokenType = 2
)

// Token is an interface that represents a SQL token.
type Token interface {
	Type() TokenType
	String() string
}

// Text represents a text token.
type Text string

// Type returns the type of the token.
// skipcq: RVV-B0013
func (t Text) Type() TokenType {
	return TextToken
}

// String returns the string representation of the token.
func (t Text) String() string {
	return string(t)
}

// Input represents an input token.
type Input string

// Type returns the type of the token.
// skipcq: RVV-B0013
func (t Input) Type() TokenType {
	return InputToken
}

// String returns the string representation of the token.
func (t Input) String() string {
	return string(t)
}

// Param represents a parameter token.
type Param string

// Type returns the type of the token.
// skipcq: RVV-B0013
func (t Param) Type() TokenType {
	return ParamToken
}

// String returns the string representation of the token.
func (t Param) String() string {
	return string(t)
}
