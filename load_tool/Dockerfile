FROM golang:1.22-alpine
LABEL authors="human"

WORKDIR /app
COPY . .

# Run go mod tidy first to ensure go.sum is properly generated
RUN go mod tidy
# Build the unified load_tool binary
RUN go build -o load_tool main.go

# Copy the default config file
COPY config.yaml /app/config.yaml

CMD ["./load_tool"]