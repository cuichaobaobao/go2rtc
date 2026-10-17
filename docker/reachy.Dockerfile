# Experimental forwarding-only image. No Python, PyAV or FFmpeg runtime.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/go2rtc .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /out/go2rtc /usr/local/bin/go2rtc
LABEL org.opencontainers.image.title="go2rtc Reachy adapter (experimental)"
LABEL org.opencontainers.image.version="0.1.0-rc.2"
USER 65532:65532
WORKDIR /tmp
EXPOSE 1984 8554 8555 8555/udp
ENTRYPOINT ["/usr/local/bin/go2rtc"]
CMD ["-config", "/config/go2rtc.yaml"]
