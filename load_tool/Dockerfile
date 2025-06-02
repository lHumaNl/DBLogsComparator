FROM golang:1.22-alpine
LABEL authors="human"

WORKDIR /app
COPY . .

RUN go mod tidy
RUN go build -o load_tool main.go