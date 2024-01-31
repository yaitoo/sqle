package sqle

import (
	"regexp"
	"sync"
)

var (
	tokenRegexp     = regexp.MustCompile(`<\w+>|\{\w+\}`)
	tokenizers      = make(map[string]*Tokenizer)
	tokenizersMutex sync.RWMutex
)

type Tokenizer struct {
	Raw    string
	Tokens []Token
}

func Tokenize(text string) *Tokenizer {
	tokenizersMutex.RLock()
	tz, ok := tokenizers[text]
	tokenizersMutex.RUnlock()
	if ok {
		return tz
	}

	indices := tokenRegexp.FindAllStringIndex(text, -1)

	tz = &Tokenizer{
		Raw: text,
	}

	start := 0
	for _, p := range indices {
		s := p[0]
		if s > start {
			tz.Tokens = append(tz.Tokens, Text(text[start:s]))
		}

		// input
		if text[s] == '<' {
			tz.Tokens = append(tz.Tokens, Input(text[s+1:p[1]-1]))
		} else {
			// params
			tz.Tokens = append(tz.Tokens, Param(text[s+1:p[1]-1]))
		}

		start = p[1]
	}

	if start < len(text) {
		tz.Tokens = append(tz.Tokens, Text(text[start:]))
	}

	tokenizersMutex.Lock()
	tokenizers[text] = tz
	tokenizersMutex.Unlock()
	return tz
}
