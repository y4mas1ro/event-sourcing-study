FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/api ./cmd/api
RUN go build -o /bin/subscriber ./cmd/subscriber

FROM alpine:3.20 AS api
COPY --from=builder /bin/api /bin/api
ENTRYPOINT ["/bin/api"]

FROM alpine:3.20 AS subscriber
COPY --from=builder /bin/subscriber /bin/subscriber
ENTRYPOINT ["/bin/subscriber"]
