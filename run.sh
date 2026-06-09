#!/usr/bin/env bash

set -x
set -e

go run -ldflags="-X 'github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=cestimator-todo'" ./app/cestimator/... \
  -config=./streams.yaml \
  -httpListenAddr=:8490 \
  -maxInsertRequestSize=500MiB