#!/usr/bin/env bash

set -x
set -e

go run -ldflags="-X 'github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=vmestimator-todo'" ./app/vmestimator/... \
  -config=./streams.yaml \
  -httpListenAddr=:8490 \
  -maxInsertRequestSize=500MiB