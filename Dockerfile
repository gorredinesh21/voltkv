# syntax=docker/dockerfile:1

# ---- build stage: compile a fully static binary with no cgo ----
FROM golang:1.23-alpine AS build
WORKDIR /src
# Copy go.mod first for layer caching (no external deps, so this is quick).
COPY go.mod ./
COPY . .
# CGO_ENABLED=0 => statically linked; -ldflags "-s -w" strips debug info to
# shrink the binary. Output lands at /out/voltkv.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /out/voltkv ./cmd/server

# ---- run stage: tiny scratch image with just the static binary ----
FROM scratch
COPY --from=build /out/voltkv /voltkv
EXPOSE 6380
# AOF disabled by default; mount a volume and pass -aof /data/voltkv.aof to enable.
ENTRYPOINT ["/voltkv", "-addr", ":6380"]
