FROM golang:1.22

WORKDIR /app
COPY go.mod go.sum .
RUN go mod download

COPY ./ ./
RUN go build main.go

EXPOSE 4195

ENTRYPOINT ["/app/main"]
