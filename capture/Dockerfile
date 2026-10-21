# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    go mod download
# Only the runtime command and its packages affect the executable. Keep the
# embedded internal/schema/market_schema.json alongside its Go package.
COPY cmd/tank-capture/ ./cmd/tank-capture/
COPY internal/ ./internal/
ARG TARGETOS
ARG TARGETARCH
# The code version stamped on every row this service writes (capture/README.md).
# Keep it after source copies; changing a revision reuses the compiler cache.
ARG GIT_SHA=dev
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=tank-capture-go-build-${TARGETOS}-${TARGETARCH},target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w -X github.com/freQuensy23-coder/Tank/capture/internal/config.Build=${GIT_SHA}" \
    -o /out/tank-capture ./cmd/tank-capture

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tank-capture /tank-capture
# UID/GID 1000 as Tank: the shared database password secret is group 1000 readable
# (docs/OPERATIONS.md, Linux secret-file ownership); the distroless nonroot user is not.
USER 1000:1000
ENTRYPOINT ["/tank-capture"]
