// Command httpdemo exposes voltkv over HTTP for platforms that only route
// HTTP (e.g. Cloud Run). It runs the real RESP server in-process on a loopback
// port and translates each HTTP request into an actual RESP client session —
// so every demo request exercises the real protocol path, not a bypass.
//
//	go run ./cmd/httpdemo            # HTTP on :8080, RESP on 127.0.0.1:16379
//	curl 'localhost:8080/set?key=greeting&value=hello'
//	curl 'localhost:8080/get?key=greeting'
package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"context"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorredinesh21/voltkv/internal/server"
)

const respAddr = "127.0.0.1:16379"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// In-process real server (TTL sweeper on, no persistence).
	go func() {
		srv := server.New(server.Config{Addr: respAddr, Shards: 16, SweepEvery: time.Second})
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("resp server:", err)
			os.Exit(1)
		}
	}()
	waitForResp()

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleHome)
	mux.HandleFunc("/set", handleSet)
	mux.HandleFunc("/get", handleGet)
	mux.HandleFunc("/info", handleInfo)

	hs := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		fmt.Println("voltkv httpdemo on :" + port + " (RESP loopback " + respAddr + ")")
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("http server:", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	hs.Close()
}

func waitForResp() {
	for i := 0; i < 50; i++ {
		if c, err := net.Dial("tcp", respAddr); err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("resp server did not come up")
	os.Exit(1)
}

// --- tiny RESP client ---

func respCommand(cmds ...string) (string, error) {
	c, err := net.DialTimeout("tcp", respAddr, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(cmds))
	for _, s := range cmds {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(s), s)
	}
	if _, err := c.Write([]byte(b.String())); err != nil {
		return "", err
	}
	return readReply(bufio.NewReader(c))
}

func readReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+', ':', '-':
		return line, nil
	case '$':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return "$-1", nil // nil bulk
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	}
	return line, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// --- HTTP handlers ---

func handleSet(w http.ResponseWriter, r *http.Request) {
	key, value := r.URL.Query().Get("key"), r.URL.Query().Get("value")
	if key == "" {
		http.Error(w, `{"error":"key required"}`, 400)
		return
	}
	reply, err := respCommand("SET", key, value)
	writeJSONorErr(w, reply, err)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"key required"}`, 400)
		return
	}
	reply, err := respCommand("GET", key)
	if err != nil {
		writeErr(w, err)
		return
	}
	if reply == "$-1" {
		fmt.Fprintf(w, `{"key":%q,"found":false}`, key)
		return
	}
	fmt.Fprintf(w, `{"key":%q,"value":%q,"found":true}`, key, reply)
}

func handleInfo(w http.ResponseWriter, r *http.Request) {
	reply, err := respCommand("INFO")
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, reply)
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, `<!doctype html><html><head><meta charset=utf-8><title>voltkv</title>
<style>body{font-family:system-ui;background:#0f1117;color:#e8eaf0;max-width:680px;margin:40px auto;padding:0 16px}a{color:#4f8cff}pre{background:#171a23;padding:12px;border-radius:8px;overflow:auto}</style></head><body>
<h1>voltkv <span style="color:#9aa3b5;font-weight:400">— Redis-compatible store in Go</span></h1>
<p>Speaks the real <b>RESP2</b> wire protocol (redis-cli compatible). This page runs the TCP server in-process on a loopback port and translates HTTP requests into actual RESP client sessions.</p>
<p>Try it:</p>
<pre>curl 'HOST/set?key=greeting&amp;value=hello'
curl 'HOST/get?key=greeting'
curl 'HOST/info'</pre>
<p><a href="https://github.com/gorredinesh21/voltkv">Source &amp; benchmarks (6.3x sharded scaling)</a></p>
</body></html>`)
}

func writeJSONorErr(w http.ResponseWriter, reply string, err error) {
	if err != nil {
		writeErr(w, err)
		return
	}
	fmt.Fprintf(w, `{"reply":%q}`, strings.TrimPrefix(reply, "+"))
}

func writeErr(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, `{"error":%q}`, err.Error())
}
