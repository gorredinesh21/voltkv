package store

import (
	"errors"
	"strconv"
)

// Errors surfaced to the command layer. They carry Redis-style messages so the
// command layer can pass them straight through as RESP errors.
var (
	// ErrWrongType matches Redis's "WRONGTYPE" reply when a command is used on a
	// key holding the wrong value type (e.g. LPUSH on a string).
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	// ErrNotInteger is returned when INCR/DECR target a non-numeric string.
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	// ErrOverflow is returned when INCR/DECR would overflow int64.
	ErrOverflow = errors.New("ERR increment or decrement would overflow")
)

const (
	maxInt64 = int64(^uint64(0) >> 1) // 9223372036854775807
	minInt64 = -maxInt64 - 1
)

func parseInt64(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

func formatInt64(n int64) string { return strconv.FormatInt(n, 10) }

// globMatch implements the subset of Redis glob-style patterns used by KEYS:
//   - '*'  matches any (possibly empty) sequence of characters
//   - '?'  matches exactly one character
//   - '[...]' matches any one of the enclosed characters (ranges like a-z allowed)
//   - '\x' escapes the next character (matches it literally)
//
// It is a small recursive/backtracking matcher — plenty fast for a demo and
// easy to read. Both pattern and string are treated as byte sequences.
func globMatch(pattern, s string) bool {
	return matchHere(pattern, s)
}

func matchHere(p, s string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			// Collapse consecutive stars, then try to match the rest of the
			// pattern at every possible position in s (classic backtracking).
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true // trailing '*' matches everything remaining
			}
			for i := 0; i <= len(s); i++ {
				if matchHere(p[1:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			p, s = p[1:], s[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			matched, rest, ok := matchClass(p, s[0])
			if !ok || !matched {
				return false
			}
			p, s = rest, s[1:]
		case '\\':
			// Escaped literal: the next pattern byte must equal the next string byte.
			if len(p) >= 2 {
				if len(s) == 0 || s[0] != p[1] {
					return false
				}
				p, s = p[2:], s[1:]
				continue
			}
			fallthrough
		default:
			if len(s) == 0 || s[0] != p[0] {
				return false
			}
			p, s = p[1:], s[1:]
		}
	}
	return len(s) == 0
}

// matchClass evaluates a "[...]" character class at the start of p against byte
// c. It returns whether c matched, the pattern remaining after the class, and
// whether the class was well-formed.
func matchClass(p string, c byte) (matched bool, rest string, ok bool) {
	// p[0] == '['
	i := 1
	negate := false
	if i < len(p) && (p[i] == '^') {
		negate = true
		i++
	}
	found := false
	for i < len(p) && p[i] != ']' {
		// Range like a-z.
		if i+2 < len(p) && p[i+1] == '-' && p[i+2] != ']' {
			lo, hi := p[i], p[i+2]
			if lo <= c && c <= hi {
				found = true
			}
			i += 3
			continue
		}
		if p[i] == c {
			found = true
		}
		i++
	}
	if i >= len(p) {
		return false, "", false // unterminated class
	}
	// Skip the closing ']'.
	return found != negate, p[i+1:], true
}
