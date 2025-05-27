FROM golang:1.22-alpine
LABEL authors="human"

WORKDIR /app
COPY . .

# Run go mod tidy first to ensure go.sum is properly generated
RUN go mod tidy
RUN go build -o go_generator main.go

CMD ["./go_generator"]