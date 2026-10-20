FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /trades_loader .

FROM gcr.io/distroless/static-debian12
COPY --from=build /trades_loader /trades_loader
ENTRYPOINT ["/trades_loader"]
