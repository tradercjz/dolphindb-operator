# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy the go source
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager cmd/operator/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o ddb-agent cmd/ddb-agent/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o ddb-dashboard cmd/ddb-dashboard/main.go

# Production stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates curl bash tzdata
WORKDIR /
COPY --from=builder /workspace/manager /manager
COPY --from=builder /workspace/ddb-agent /ddb-agent
COPY --from=builder /workspace/ddb-dashboard /ddb-dashboard

USER 65532:65532

ENTRYPOINT ["/manager"]
