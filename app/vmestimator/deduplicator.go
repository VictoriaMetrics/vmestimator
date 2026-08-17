package main

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/vmestimator/app/vmestimator/protoparser"
	"github.com/axiomhq/hyperloglog"
)

var (
	deduplicationInterval    = flag.Duration("deduplication.interval", 0, "Time window for deduplication. When set, time series already seen within the window are dropped before being forwarded to estimators. Disabled when set to 0.")
	deduplicatorMaxSizeBytes = flagutil.NewBytes("deduplication.maxSizeBytes", 0, "Memory budget for deduplication bloom filters, e.g. 100MB. "+
		"Controls how many unique time series can be tracked before the deduplicator gradually switches to pass-through mode. "+
		"Pass-through starts at 80%% of the limit to keep bloom filters well below saturation. "+
		"Defaults to 5 percent of allowed memory. When both -deduplication.maxSizeBytes and -deduplication.maxSize are set, the stricter limit is used.")
	deduplicatorMaxSize = flag.Int("deduplication.maxSize", 0, "Maximum number of unique time series the deduplicator tracks. "+
		"Pass-through begins at 80%% of this limit so bloom filters never saturate and start producing false positives. "+
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

	// HLL sketches for estimating the true unique-series count.
	skMu   sync.Mutex
	currSk *hyperloglog.Sketch
	prevSk *hyperloglog.Sketch

	// Gradual pass-through ratio stored per-1000.
	// 0 = deduplicate all; 1000 = pass everything through.
	// When unique > maxSize, ratio = (unique - maxSize) * 1000 / unique.
	passthroughPer1000 atomic.Int64

	stopCh chan struct{}
}

func newDeduplicator(maxSize int, interval time.Duration) *deduplicator {
	if maxSize <= 0 {
		panic("BUG: maxSize must be greater than 1000")
	}
	if interval <= 0 {
		panic("BUG: interval must be greater than 30s")
	}

	d := &deduplicator{
		maxSize:       maxSize,
		interval:      interval,
		bucketMaxSize: maxSize / 10,
		currSk:        newDeduplicatorSketch(),
		prevSk:        newDeduplicatorSketch(),
		stopCh:        make(chan struct{}),
	}

	for i := range d.bfs {
		d.bfs[i].Store(newDeduplicatorBloomFilter(d.bucketMaxSize))
	}

	go d.runRotation()
	return d
}

// filter filters out time series already seen within the deduplication interval.
// src is the incoming batch; dst is the destination slice (may be nil or pre-allocated).
// The returned slice contains only time series that should be forwarded to estimators.
func (d *deduplicator) filter(src, dst []protoparser.TimeSerie) []protoparser.TimeSerie {
	// Update HLL with every incoming series before filtering, so cardinality
	// estimates reflect true inbound volume regardless of deduplication decisions.
	d.skMu.Lock()
	for i := range src {
		d.currSk.InsertHash(src[i].Fingerprint)
	}
	d.skMu.Unlock()

	passthrough := d.passthroughPer1000.Load()

	for i := range src {
		ts := src[i]
		h := ts.Fingerprint

		// Overflow pass-through: deterministic by hash so the same series
		// always lands on the same side of the split.
		if passthrough > 0 && int64(h%1000) < passthrough {
			dst = append(dst, ts)
			continue
		}

		bf := d.bfs[h%10].Load()
		if !bf.contains(h) {
			bf.add(h)
			dst = append(dst, ts)
		}
	}
	return dst
}

func (d *deduplicator) Stop() {
	close(d.stopCh)
}

func (d *deduplicator) runRotation() {
	bucketDur := d.interval / 10
	var prevRatio int64

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

		// Recalculate pass-through ratio after each bucket rotation.
		d.skMu.Lock()
		resSk := newDeduplicatorSketch()
		_ = resSk.Merge(d.currSk)
		_ = resSk.Merge(d.prevSk)
		d.skMu.Unlock()
		unique := resSk.Estimate()

		// Start pass-through at 80% of maxSize so the bloom filters never
		// reach saturation: at full capacity false-positive rate spikes,
		// which would silently drop legitimate new series.
		passthroughThreshold := uint64(d.maxSize) * 4 / 5
		var ratio int64
		if unique > passthroughThreshold {
			ratio = int64((unique - passthroughThreshold) * 1000 / unique)
		}
		d.passthroughPer1000.Store(ratio)

		// Rotate HLL sketches once per full interval (every 10 bucket rotations).
		if idx == 0 {
			newSk := newDeduplicatorSketch()
			d.skMu.Lock()
			d.prevSk = d.currSk
			d.currSk = newSk
			d.skMu.Unlock()
		}

		if ratio != prevRatio {
			prevRatio = ratio
			logger.Infof("deduplicator: unique series estimate=%d maxSize=%d passthrough=%d/1000", unique, d.maxSize, ratio)
		}
	}
}

const hashesCount = 4
const bitsPerItem = 16

type deduplicatorBloomFilter struct {
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
	return isNew
}

func newBits(maxItems uint64) []uint64 {
	bitsCount := maxItems * bitsPerItem
	return make([]uint64, (bitsCount+63)/64)
}

func newDeduplicatorSketch() *hyperloglog.Sketch {
	sk, err := hyperloglog.NewSketch(12, false)
	if err != nil {
		panic(fmt.Sprintf("BUG: hyperloglog: new sketch: %s", err))
	}
	return sk
}

func convertDeduplicatorMaxSizeBytesToItems(bytes int) int {
	// Subtract memory used by the two HLL sketches (2 * 2^12 bytes).
	skBytes := 2 * (1 << 12)
	bytes -= skBytes
	if bytes <= 0 {
		return 0
	}
	return bytes * 8 / bitsPerItem
}
