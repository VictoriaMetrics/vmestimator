module github.com/VictoriaMetrics/vmestimator

go 1.27.3

replace github.com/axiomhq/hyperloglog => github.com/makasim/hyperloglog v0.0.14-reuse-memory

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.154.1
	github.com/VictoriaMetrics/easyproto v1.2.0
	github.com/VictoriaMetrics/metrics v1.44.1
	github.com/VictoriaMetrics/metricsql v0.87.5
	github.com/axiomhq/hyperloglog v0.0.14-reuse-memory
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgryski/go-metro v0.0.0-20261008173524-5059e0b1da03
	github.com/golang/snappy v1.0.0
	github.com/valyala/fastrand v1.1.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/kamstrup/intmap v0.5.2 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/gozstd v1.26.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
