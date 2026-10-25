# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
COPY capture/go.mod capture/go.sum ./
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    go mod download
COPY capture/cmd/ ./cmd/
COPY capture/internal/ ./internal/
ARG TARGETOS
ARG TARGETARCH
ARG GIT_SHA=dev
RUN --mount=type=cache,id=tank-capture-go-mod,target=/go/pkg/mod \
    --mount=type=cache,id=tank-capture-go-build-${TARGETOS}-${TARGETARCH},target=/root/.cache/go-build \
    if [ -z "$GIT_SHA" ]; then printf '%s\n' 'GIT_SHA must not be empty; use dev for development' >&2; exit 1; fi; \
    GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w -X github.com/freQuensy23-coder/Tank/capture/internal/config.Build=${GIT_SHA}" \
    -o /out/tank-capture ./cmd/tank-capture
COPY config/ /out/config/

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tank-capture /tank-capture
COPY --from=build /out/config/ /config/
ENV TANK_CONFIG_DIR=/config \
    TANK_CAPTURE_HEALTH_ADDR=:8791 \
    TANK_CLICKHOUSE_URL=http://tank@clickhouse:8123
USER 1000:1000
HEALTHCHECK --interval=15s --timeout=5s --retries=4 --start-period=60s CMD ["/tank-capture", "healthcheck"]
ENTRYPOINT ["/tank-capture"]
