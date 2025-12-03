FROM golang:1.25.4-alpine AS builder
WORKDIR /build
ADD . /build/
RUN mkdir /out
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod/ go build -o /out/service .

FROM alpine
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/service /app

ENTRYPOINT ["/app/service"] 
