FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.work go.work.sum* ./
COPY ingestion/go.mod ingestion/go.sum* ./ingestion/
COPY batching/go.mod ./batching/
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /batching-app batching/cmd/main.go

FROM alpine:latest

WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /batching-app /app/batching-app

CMD ["/app/batching-app"]