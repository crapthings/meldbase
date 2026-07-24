# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/meld ./cmd/meld

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/meld /usr/local/bin/meld
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/meld"]
CMD ["--help"]
