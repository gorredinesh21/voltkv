# make.ps1 — task runner for voltkv on Windows (the portable Go SDK is not on PATH).
#
# Usage:
#   .\make.ps1 build        # compile ./voltkv.exe (static, no cgo)
#   .\make.ps1 test         # go test -timeout 60s ./...
#   .\make.ps1 bench        # sharding benchmark
#   .\make.ps1 run          # start server on :6380 with AOF enabled
#   .\make.ps1 vet          # go vet ./...
#   .\make.ps1 clean        # remove binary + .aof files
#   .\make.ps1 docker       # docker build
#
# It bakes in the portable-SDK environment so you don't have to set it each time.

param(
    [Parameter(Position = 0)]
    [string]$Target = "build"
)

# Point at the portable Go SDK and disable cgo (no C compiler on this box).
$env:GOROOT = "$env:USERPROFILE\go-sdk\go"
$env:PATH = "$env:GOROOT\bin;$env:PATH"
$env:CGO_ENABLED = "0"

$pkg = "./cmd/server"
$binary = "voltkv.exe"

switch ($Target.ToLower()) {
    "build" { go build -ldflags "-s -w" -o $binary $pkg }
    "test" { go test -timeout 60s ./... }
    "bench" { go test -bench BenchmarkSetShards -benchmem ./internal/store }
    "run" { go run $pkg -addr :6380 -aof voltkv.aof }
    "vet" { go vet ./... }
    "clean" {
        Remove-Item -Force -ErrorAction SilentlyContinue $binary, *.aof, *.aof.rewrite
        Write-Host "cleaned"
    }
    "docker" { docker build -t voltkv:latest . }
    default { Write-Host "unknown target '$Target'. Use: build|test|bench|run|vet|clean|docker" }
}
