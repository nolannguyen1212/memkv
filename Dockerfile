FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags "-s -w" -o /memkv ./cmd/memkv

FROM alpine
WORKDIR /
COPY --from=builder /memkv /memkv
COPY config.yaml /config.yaml
EXPOSE 6379
USER 65532:65532
ENTRYPOINT ["/memkv"]
