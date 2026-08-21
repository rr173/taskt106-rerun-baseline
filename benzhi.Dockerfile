# Official Go image with the project-pinned toolchain.
FROM golang:1.26.3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build ./...

CMD ["bash"]
