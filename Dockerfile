# --- Build Stage ---
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/gopherload ./cmd/gopherload
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/fakebackend ./cmd/fakebackend

# --- Run Stage ---
FROM alpine:latest
RUN apk --no-cache add ca-certificates

# Copy the binaries from the builder
COPY --from=builder /bin/gopherload /usr/local/bin/
COPY --from=builder /bin/fakebackend /usr/local/bin/

# Default port for the proxy
EXPOSE 8080 9090

ENTRYPOINT ["gopherload"]
