FROM golang:1.24.9-bookworm AS build
ENV CGO_ENABLED=0 GOBIN=/out GOTOOLCHAIN=local
RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z
RUN GOMAXPROCS=2 go install -p 2 -ldflags="-s -w -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-10-15T17-29-55Z -X github.com/minio/minio/cmd.CommitID=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
    github.com/minio/minio@9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a

FROM alpine:3.22.2
LABEL org.opencontainers.image.version="RELEASE.2025-10-15T17-29-55Z" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a"
RUN apk add --no-cache ca-certificates && addgroup -g 10001 minio && adduser -D -u 10001 -G minio minio \
    && mkdir /data && chown minio:minio /data
COPY --from=build /out/minio /usr/local/bin/minio
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
