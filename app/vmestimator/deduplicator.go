package main

import (
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/metrics"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
)

var (
	deduplicationPassedTotal  = metrics.NewCounter(`vmestimator_deduplication_passed_total`)
	deduplicationDroppedTotal = metrics.NewCounter(`vmestimator_deduplication_dropped_total`)
)

var (
	deduplicationInterval    = flag.Duration("deduplication.interval", 0, "Time window for deduplication. When set, time series already seen within the window are dropped before being forwarded to estimators. Disabled when set to 0.")
	deduplicatorMaxSizeBytes = flagutil.NewBytes("deduplication.maxSizeBytes", 0, "Memory budget for deduplication bloom filters, e.g. 100MB. "+
		"Controls how many unique time series can be tracked before the deduplicator gradually switches to pass-through mode. "+
		"Pass-through starts at 80% of the limit to keep bloom filters well below saturation. "+
		"Defaults to 5 percent of allowed memory. When both -deduplication.maxSizeBytes and -deduplication.maxSize are set, the stricter limit is used.")
	deduplicatorMaxSize = flag.Int("deduplication.maxSize", 0, "Maximum number of unique time series the deduplicator tracks. "+
		"Pass-through begins at 80% of this limit so bloom filters never saturate and start producing false positives. "+
		"Defaults to a value derived from 5 percent of allowed memory. When both -deduplication.maxSizeBytes and -deduplication.maxSize are set, the stricter limit is used.")
)

// resolveDeduplicatorMaxSize returns the maximum number of unique time series
// the deduplicator will track, derived from the configured flags.
func resolveDeduplicatorMaxSize() int {
	fromBytes := 0
	if deduplicatorMaxSizeBytes.N > 0 {
		fromBytes = convertDeduplicatorMaxSizeBytesToItems(int(deduplicatorMaxSizeBytes.N))
	}
	fromCount := *deduplicatorMaxSize

	switch {
	case fromBytes > 0 && fromCount > 0:
		return min(fromBytes, fromCount)
	case fromBytes > 0:
		return fromBytes
	case fromCount > 0:
		return fromCount
	default:
		// Default: 5% of allowed memory.
		allowed := int(float64(memory.Allowed()) * 0.05)
		return convertDeduplicatorMaxSizeBytesToItems(allowed)
	}
}

type deduplicator struct {
	maxSize       int
	interval      time.Duration
	bucketMaxSize int

	// 10 independent bloom filter buckets.
	// Each series maps to bucket fingerprint%10.
	// Rotation clears one bucket per interval/10 on a fixed-grid schedule,
	// smoothing CPU spikes across time.
	bfs [10]atomic.Pointer[deduplicatorBloomFilter]

	stopCh chan struct{}
}

const (
	deduplicationMinMaxSize  = 9999
	deduplicationMinInterval = time.Second * 30
)

func newDeduplicator(maxSize int, interval time.Duration) *deduplicator {
	if maxSize <= deduplicationMinMaxSize {
		panic(fmt.Sprintf("BUG: maxSize must be greater than %d; got %d", deduplicationMinMaxSize, maxSize))
	}
	if interval <= deduplicationMinInterval {
		panic(fmt.Sprintf("BUG: interval must be greater than %v; got %v", deduplicationMinInterval, interval))
	}

	d := &deduplicator{
		maxSize:       maxSize,
		interval:      interval,
		bucketMaxSize: maxSize / 10,
		stopCh:        make(chan struct{}),
	}

	for i := range d.bfs {
		d.bfs[i].Store(newDeduplicatorBloomFilter(d.bucketMaxSize))
	}

	metrics.NewGauge(`vmestimator_deduplication_bloom_filter_max_size`, func() float64 {
		return float64(d.maxSize)
	})
	metrics.NewGauge(`vmestimator_deduplication_bloom_filter_size`, func() float64 {
		var total int64
		for i := range d.bfs {
			total += d.bfs[i].Load().size.Load()
		}
		return float64(total)
	})

	go d.runRotation()
	return d
}

// filter filters out time series already seen within the deduplication interval.
// src is the incoming batch; dst is the destination slice (may be nil or pre-allocated).
// The returned slice contains only time series that should be forwarded to estimators.
func (d *deduplicator) filter(src, dst []protoparser.TimeSerie) []protoparser.TimeSerie {
	if len(src) == 0 {
		return dst
	}

	for i := range src {
		ts := src[i]
		h := ts.Fingerprint

		bf := d.bfs[h%10].Load()
		if bf.contains(h) {
			deduplicationDroppedTotal.Inc()
			continue
		}

		bfSize := bf.size.Load()
		if bfSize < int64(d.bucketMaxSize) {
			bf.add(h)
		}
		deduplicationPassedTotal.Inc()
		dst = append(dst, ts)
	}
	return dst
}

func (d *deduplicator) Stop() {
	metrics.UnregisterMetric(`vmestimator_deduplication_bloom_filter_max_size`)
	metrics.UnregisterMetric(`vmestimator_deduplication_bloom_filter_size`)
	close(d.stopCh)
}

func (d *deduplicator) runRotation() {
	bucketDur := d.interval / 10

	for {
		now := time.Now()
		nextBoundary := now.Truncate(bucketDur).Add(bucketDur)
		t := time.NewTimer(nextBoundary.Sub(now))
		select {
		case <-t.C:
		case <-d.stopCh:
			t.Stop()
			return
		}

		idx := (nextBoundary.UnixNano() / int64(bucketDur)) % 10
		d.bfs[idx].Store(newDeduplicatorBloomFilter(d.bucketMaxSize))
	}
}

const hashesCount = 4
const bitsPerItem = 16

type deduplicatorBloomFilter struct {
	size atomic.Int64
	bits []uint64
}

func newDeduplicatorBloomFilter(maxItems int) *deduplicatorBloomFilter {
	return &deduplicatorBloomFilter{
		bits: newBits(uint64(maxItems)),
	}
}

// contains returns true if h was previously added to this filter.
func (f *deduplicatorBloomFilter) contains(h uint64) bool {
	bits := f.bits
	maxBits := uint64(len(bits)) * 64
	h1 := h & 0xFFFFFFFF
	h2 := h >> 32
	for i := uint64(0); i < hashesCount; i++ {
		idx := (h1 + i*h2) % maxBits
		word := idx / 64
		bit := idx % 64
		mask := uint64(1) << bit
		if atomic.LoadUint64(&bits[word])&mask == 0 {
			return false
		}
	}
	return true
}

// add adds h to the filter. Returns true if the item appears to be new
// (at least one bit needed to be set).
func (f *deduplicatorBloomFilter) add(h uint64) bool {
	bits := f.bits
	maxBits := uint64(len(bits)) * 64

	// Kirsch-Mitzenmacher double hashing: derive k indices from two halves of h
	// using h_i = (h1 + i*h2) % maxBits, avoiding k separate hash computations.
	h1 := h & 0xFFFFFFFF
	h2 := h >> 32

	isNew := false
	for i := uint64(0); i < hashesCount; i++ {
		idx := (h1 + i*h2) % maxBits
		word := idx / 64
		bit := idx % 64
		mask := uint64(1) << bit
		w := atomic.LoadUint64(&bits[word])
		for w&mask == 0 {
			// The w|mask != w most of the time, so there is no need in using atomic.LoadUint64
			// in front of atomic.CompareAndSwapUint64 in order to try avoiding slow inter-CPU synchronization.
			if atomic.CompareAndSwapUint64(&bits[word], w, w|mask) {
				isNew = true
				break
			}
			w = atomic.LoadUint64(&bits[word])
		}
	}

	if isNew {
		f.size.Add(1)
	}
	return isNew
}

func newBits(maxItems uint64) []uint64 {
	bitsCount := maxItems * bitsPerItem
	return make([]uint64, (bitsCount+63)/64)
}

func convertDeduplicatorMaxSizeBytesToItems(bytes int) int {
	return bytes * 8 / bitsPerItem
}
