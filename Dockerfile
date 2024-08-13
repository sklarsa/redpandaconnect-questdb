FROM golang:1.22 AS build

WORKDIR /app
COPY go.mod go.sum .
RUN go mod download

COPY ./ ./
RUN go build main.go

FROM ubuntu:jammy

WORKDIR /app
COPY --from=build /app/main /app/main

EXPOSE 4195

ENTRYPOINT ["/app/main"]
