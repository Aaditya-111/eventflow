FROM golang:1.26.5-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/eventflow ./cmd/server

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S eventflow \
    && adduser -S -D -H -G eventflow eventflow

WORKDIR /app

COPY --from=builder --chown=eventflow:eventflow /out/eventflow /app/eventflow

USER eventflow

EXPOSE 8080

ENTRYPOINT ["/app/eventflow"]
