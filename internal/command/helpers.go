package command

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var errSyntax = errors.New("ERR syntax error")

// parseSetOpts parses the optional trailing options of SET:
//
//	[EX seconds | PX milliseconds] [NX | XX]
//
// EX/PX are mutually exclusive (only one TTL), and NX/XX are mutually exclusive.
// Options may appear in either order. Unknown tokens are a syntax error.
func parseSetOpts(opts []string) (ttl time.Duration, nx, xx bool, err error) {
	i := 0
	haveTTL := false
	for i < len(opts) {
		switch strings.ToUpper(opts[i]) {
		case "EX", "PX":
			if haveTTL || i+1 >= len(opts) {
				return 0, false, false, errSyntax
			}
			amount, perr := parsePositiveInt(opts[i+1])
			if perr != nil || amount <= 0 {
				return 0, false, false, errSyntax
			}
			if strings.ToUpper(opts[i]) == "EX" {
				ttl = time.Duration(amount) * time.Second
			} else {
				ttl = time.Duration(amount) * time.Millisecond
			}
			haveTTL = true
			i += 2
		case "NX":
			if xx {
				return 0, false, false, errSyntax
			}
			nx = true
			i++
		case "XX":
			if nx {
				return 0, false, false, errSyntax
			}
			xx = true
			i++
		default:
			return 0, false, false, errSyntax
		}
	}
	return ttl, nx, xx, nil
}

func parsePositiveInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, errSyntax
	}
	return n, nil
}
