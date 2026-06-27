module github.com/VictoriaMetrics/vmestimator

go 1.26.4

replace github.com/axiomhq/hyperloglog => github.com/makasim/hyperloglog v0.0.10-reuse-memory

//replace github.com/axiomhq/hyperloglog => /Users/makasim/projects/VictoriaMetrics/hyperloglog

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.146.0
	github.com/VictoriaMetrics/easyproto v1.2.0
	github.com/VictoriaMetrics/metrics v1.44.0
	github.com/axiomhq/hyperloglog v0.0.10-reuse-memory
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33
	github.com/golang/snappy v1.0.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/VictoriaMetrics/metricsql v0.87.1 // indirect
	github.com/kamstrup/intmap v0.5.2 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/gozstd v1.24.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	github.com/valyala/quicktemplate v1.8.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
)
