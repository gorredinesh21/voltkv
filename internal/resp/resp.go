// Package resp implements just enough of the Redis RESP2 wire protocol for a
// server to parse client commands and write replies. Clients send commands as
// RESP arrays of bulk strings; servers reply with simple strings, errors,
// integers, bulk strings, or null.
//
// Reference: https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Reader parses incoming RESP commands from a client connection.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReader(r)} }

// ReadCommand reads one command, returned as its argument slice, e.g.
// ["SET", "foo", "bar"]. Real clients always send commands as an array of bulk
// strings, but we also accept inline commands to be friendly to `nc`/telnet.
func (r *Reader) ReadCommand() ([]string, error) {
	prefix, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '*':
		return r.readArray()
	default:
		// Inline command: put the byte back and read a whitespace-split line.
		_ = r.r.UnreadByte()
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		return splitInline(line), nil
	}
}

func (r *Reader) readArray() ([]string, error) {
	n, err := r.readInt()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		b, err := r.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != '$' {
			return nil, fmt.Errorf("resp: expected bulk string, got %q", b)
		}
		length, err := r.readInt()
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		if _, err := r.r.Discard(2); err != nil { // trailing CRLF
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// strip trailing \r\n (or \n)
	for len(line) > 0 && (line[len(line)-1] == '\n' || line[len(line)-1] == '\r') {
		line = line[:len(line)-1]
	}
	return line, nil
}

func (r *Reader) readInt() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(line)
}

func splitInline(line string) []string {
	var out []string
	var cur []rune
	for _, ch := range line {
		if ch == ' ' {
			if len(cur) > 0 {
				out = append(out, string(cur))
				cur = cur[:0]
			}
			continue
		}
		cur = append(cur, ch)
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

// Writer encodes RESP2 replies back to the client.
type Writer struct {
	w *bufio.Writer
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriter(w)} }

func (w *Writer) Flush() error { return w.w.Flush() }

// WriteSimpleString writes "+OK\r\n" style replies.
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

// WriteError writes "-ERR message\r\n".
func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s\r\n", msg)
	return err
}

// WriteInteger writes ":123\r\n".
func (w *Writer) WriteInteger(n int) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

// WriteInteger64 writes a 64-bit integer reply (used by INCR/DECR which can
// exceed the int range on 32-bit platforms).
func (w *Writer) WriteInteger64(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

// WriteBulkString writes a bulk string like "$3\r\nbar\r\n".
func (w *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteNull writes the RESP2 null bulk string "$-1\r\n" (used for GET miss).
func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// WriteNullArray writes the RESP2 null array "*-1\r\n" (e.g. LRANGE on a missing
// key returns an empty array, but some commands use a null array).
func (w *Writer) WriteNullArray() error {
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

// WriteArrayHeader writes the "*<n>\r\n" prefix for an array of n elements. The
// caller then writes exactly n elements with the other Write* methods. Splitting
// the header out lets us stream arrays of mixed element types (e.g. pub/sub push
// replies mix bulk strings and integers).
func (w *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.w, "*%d\r\n", n)
	return err
}

// WriteStringArray writes an array where every element is a bulk string. Used by
// MGET, HGETALL, HKEYS, HVALS, LRANGE, KEYS, etc.
func (w *Writer) WriteStringArray(items []string) error {
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, it := range items {
		if err := w.WriteBulkString(it); err != nil {
			return err
		}
	}
	return nil
}

// WriteNullableStringArray writes a bulk-string array where a nil entry becomes a
// RESP null (used by MGET, which returns null for missing keys). Pass ok[i]=false
// to emit a null for element i.
func (w *Writer) WriteNullableStringArray(items []string, ok []bool) error {
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for i := range items {
		if ok[i] {
			if err := w.WriteBulkString(items[i]); err != nil {
				return err
			}
		} else {
			if err := w.WriteNull(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrEmptyCommand is returned by dispatchers when the client sends no args.
var ErrEmptyCommand = errors.New("empty command")
