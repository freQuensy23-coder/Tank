# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    go mod download
COPY cmd/tank-capture/ ./cmd/tank-capture/
COPY internal/ ./internal/
ARG TARGETOS
ARG TARGETARCH
ARG GIT_SHA=dev
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=tank-capture-go-build-${TARGETOS}-${TARGETARCH},target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w -X github.com/freQuensy23-coder/Tank/capture/internal/config.Build=${GIT_SHA}" \
    -o /out/tank-capture ./cmd/tank-capture

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tank-capture /tank-capture
USER 1000:1000
ENTRYPOINT ["/tank-capture"]
