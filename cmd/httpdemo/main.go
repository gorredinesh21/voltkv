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
		if err := srv.Run(context.Background()); err != nil {
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
	fmt.Fprint(w, `<!doctype html><html><head><meta charset=utf-8><title>voltkv — try it live</title>
<style>
body{font-family:system-ui;background:#0f1117;color:#e8eaf0;max-width:680px;margin:40px auto;padding:0 16px}
input{background:#171a23;color:#e8eaf0;border:1px solid #2a2f3d;border-radius:8px;padding:10px 14px;font-size:14px;outline:none}
input:focus{border-color:#4f8cff}
button{background:#4f8cff;color:#fff;border:0;border-radius:8px;padding:10px 20px;font-size:14px;cursor:pointer}
button:disabled{opacity:.4}
.result{background:#171a23;padding:12px 16px;border-radius:8px;margin-top:10px;font-family:monospace;font-size:13px;white-space:pre-wrap;min-height:2em}
label{color:#9aa3b5;font-size:12px;margin-bottom:4px;display:block}
.row{display:flex;gap:8px;margin-bottom:12px;align-items:flex-end}
.half{flex:1}
a{color:#4f8cff}
h1 span{color:#9aa3b5;font-weight:400;font-size:18px}
.note{color:#9aa3b5;font-size:12px;margin-top:6px}
</style></head><body>
<h1>voltkv <span>— Redis-compatible store in Go</span></h1>
<p style="color:#9aa3b5">This page runs the real TCP server in-process. Each button below sends an actual RESP client command. Try storing and retrieving values.</p>
<div class="row"><div class="half"><label>Key</label><input id="key" placeholder="mykey" style="width:100%"></div>
<div class="half"><label>Value</label><input id="value" placeholder="hello world" style="width:100%"></div></div>
<div class="row"><button onclick="doSet()" id="btnSet">SET key = value</button><button onclick="doGet()" id="btnGet">GET key</button><button onclick="doInfo()">Server INFO</button></div>
<div class="result" id="result">— click a button above —</div>
<p class="note">Also works via curl: <code>curl 'HOST/set?key=greeting&value=hello'</code> · <a href="https://github.com/gorredinesh21/voltkv">Source &amp; benchmarks</a></p>
<script>
async function call(path){const r=await fetch(path);return r.json()}
async function doSet(){const k=document.getElementById('key').value||'mykey';const v=document.getElementById('value').value||'hello';
document.getElementById('result').textContent='Setting...';const d=await call('/set?key='+encodeURIComponent(k)+'&value='+encodeURIComponent(v));
document.getElementById('result').textContent='SET → '+JSON.stringify(d)}
async function doGet(){const k=document.getElementById('key').value||'mykey';
document.getElementById('result').textContent='Getting...';const d=await call('/get?key='+encodeURIComponent(k));
document.getElementById('result').textContent='GET → '+JSON.stringify(d,null,2)}
async function doInfo(){document.getElementById('result').textContent='Loading...';
const r=await fetch('/info');document.getElementById('result').textContent=await r.text()}
</script></body></html>`)
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
