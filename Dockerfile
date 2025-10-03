FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags "-s -w" -o /valkeydb ./cmd/valkeydb

FROM alpine
WORKDIR /
COPY --from=builder /valkeydb /valkeydb
COPY config.yaml /config.yaml
EXPOSE 6379
USER 65532:65532
ENTRYPOINT ["/valkeydb"]
