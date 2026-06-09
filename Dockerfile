FROM golang:1.26.2-alpine3.23 AS builder

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /build
COPY . .

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-X 'github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=cestimator-todo'" -o cestimator ./app/cestimator

FROM public.ecr.aws/docker/library/alpine:3.23

COPY --from=builder /build/cestimator /cestimator

ENTRYPOINT ["/cestimator"]
