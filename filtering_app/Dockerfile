FROM golang:1.25.3-bookworm AS builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download
COPY ./ ./
RUN go build -o /kafka-app


FROM debian:bookworm-20250908

COPY --from=builder /kafka-app /app/kafka-app

ENTRYPOINT ["/app/kafka-app"]
