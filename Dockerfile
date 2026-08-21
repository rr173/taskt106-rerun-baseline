FROM docker.m.daocloud.io/library/golang:1.26.3-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN GOTOOLCHAIN=local GOPROXY=https://goproxy.cn,direct GOSUMDB=sum.golang.google.cn go mod download

COPY . .

RUN CGO_ENABLED=0 GOTOOLCHAIN=local GOOS=linux go build -o /lock-server ./cmd/lock-server

FROM docker.m.daocloud.io/library/alpine:3.20

WORKDIR /app

COPY --from=builder /lock-server /app/lock-server

RUN mkdir -p /app/data

ENV DB_PATH=/app/data/locks.db
ENV ADDR=:8080

EXPOSE 8080

ENTRYPOINT ["/app/lock-server"]
