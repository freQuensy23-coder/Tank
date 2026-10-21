# syntax=docker/dockerfile:1.7
FROM golang:1.25-bookworm AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The code version stamped on every row this service writes (capture/README.md).
ARG GIT_SHA=dev
RUN go build -trimpath \
    -ldflags "-s -w -X github.com/freQuensy23-coder/Tank/capture/internal/config.Build=${GIT_SHA}" \
    -o /out/tank-capture ./cmd/tank-capture

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tank-capture /tank-capture
# UID/GID 1000 as Tank: the shared database password secret is group 1000 readable
# (docs/OPERATIONS.md, Linux secret-file ownership); the distroless nonroot user is not.
USER 1000:1000
ENTRYPOINT ["/tank-capture"]
