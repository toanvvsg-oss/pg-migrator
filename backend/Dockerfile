FROM golang:1.23.4-alpine3.20 AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pg-migrator ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache postgresql16-client ca-certificates tzdata && \
    adduser -D -u 10001 appuser

COPY --from=builder /out/pg-migrator /usr/local/bin/pg-migrator

USER appuser
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/pg-migrator"]
