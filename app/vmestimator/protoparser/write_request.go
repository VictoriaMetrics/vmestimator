package protoparser

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/easyproto"
	"github.com/cespare/xxhash/v2"
)

type TimeSerie struct {
	Labels      []Label
	Fingerprint uint64
}

type Label struct {
	Name  string
	Value string

	// Contains fingerprint of Value.
	// Calculated only if labelFP given to Parse function is true
	Fingerprint uint64
}

type WriteRequest struct {
	// Timeseries is a list of time series in the given WriteRequest
	Timeseries []TimeSerie
}

// Reset resets wr for subsequent reuse.
func (wr *WriteRequest) Reset() {
	wr.Timeseries = ResetTimeSeries(wr.Timeseries)
}

func getWriteRequestUnmarshaler() *writeRequestUnmarshaler {
	v := wruPool.Get()
	if v == nil {
		return &writeRequestUnmarshaler{
			labelsPool: make([]Label, 0, 4096),
		}
	}
	return v.(*writeRequestUnmarshaler)
}

func putWriteRequestUnmarshaler(wru *writeRequestUnmarshaler) {
	wru.Reset()
	wruPool.Put(wru)
}

var wruPool sync.Pool

// WriteRequestUnmarshaler is reusable unmarshaler for WriteRequest protobuf messages.
//
// It maintains internal pools for labels and samples to reduce memory allocations.
// See UnmarshalProtobuf for details on how to use it.
type writeRequestUnmarshaler struct {
	wr WriteRequest

	labelsPool []Label
}

// Reset resets wru, so it could be re-used.
func (wru *writeRequestUnmarshaler) Reset() {
	wru.wr.Reset()

	clear(wru.labelsPool)
	wru.labelsPool = wru.labelsPool[:0]
}

func (wru *writeRequestUnmarshaler) UnmarshalProtobuf(src []byte) (*WriteRequest, error) {
	wru.Reset()

	fpLabels := fpLabelsGlobal.Load()

	var err error

	// message WriteRequest {
	//    repeated TimeSeries timeseries = 1;
	//    reserved 2;
	//    repeated Metadata metadata = 3;
	// }
	tss := wru.wr.Timeseries
	labelsPool := wru.labelsPool
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return nil, fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return nil, fmt.Errorf("cannot read timeseries data")
			}
			if len(tss) < cap(tss) {
				tss = tss[:len(tss)+1]
			} else {
				tss = append(tss, TimeSerie{})
			}
			ts := &tss[len(tss)-1]
			labelsPool, err = ts.unmarshalProtobuf(data, labelsPool, fpLabels)
			if err != nil {
				return nil, fmt.Errorf("cannot unmarshal timeseries: %w", err)
			}
		}
	}
	wru.wr.Timeseries = tss
	wru.labelsPool = labelsPool
	return &wru.wr, nil
}

func (ts *TimeSerie) unmarshalProtobuf(src []byte, labelsPool []Label, fpLabels bool) ([]Label, error) {
	// message TimeSeries {
	//   repeated Label labels   = 1;
	//   repeated Sample samples = 2;
	// }

	tsD := getDigest()
	defer putDigest(tsD)

	digestLabel := func(value []byte) uint64 {
		return 0
	}
	if fpLabels {
		ld := getDigest()
		defer putDigest(ld)

		digestLabel = func(value []byte) uint64 {
			ld.Reset()
			_, _ = ld.Write(value)
			return ld.Sum64()
		}
	}

	labelsPoolLen := len(labelsPool)
	var fc easyproto.FieldContext
	var lfc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return labelsPool, fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return labelsPool, fmt.Errorf("cannot read label data")
			}

			var nameBytes, valueBytes []byte
			ldata := data
			for len(ldata) > 0 {
				ldata, err = lfc.NextField(ldata)
				if err != nil {
					return labelsPool, fmt.Errorf("cannot read label field: %w", err)
				}
				switch lfc.FieldNum {
				case 1:
					nameBytes, ok = lfc.Bytes()
					if !ok {
						return labelsPool, fmt.Errorf("cannot read label name")
					}
				case 2:
					valueBytes, ok = lfc.Bytes()
					if !ok {
						return labelsPool, fmt.Errorf("cannot read label value")
					}
				}
			}

			_, _ = tsD.Write(data)
			labelsPool = append(labelsPool, Label{
				Name:        bytesutil.ToUnsafeString(nameBytes),
				Value:       bytesutil.ToUnsafeString(valueBytes),
				Fingerprint: digestLabel(valueBytes),
			})
		}
	}
	ts.Labels = labelsPool[labelsPoolLen:]
	ts.Fingerprint = tsD.Sum64()
	return labelsPool, nil
}

func getDigest() *xxhash.Digest {
	return xxhashPool.Get().(*xxhash.Digest)
}

func putDigest(d *xxhash.Digest) {
	d.Reset()
	xxhashPool.Put(d)
}

var xxhashPool = &sync.Pool{
	New: func() any {
		return xxhash.New()
	},
}

var fpLabelsGlobal atomic.Bool

func SetFingerprintLabels(b bool) {
	fpLabelsGlobal.Store(b)
}

// ResetTimeSeries clears all the GC references from tss and returns an empty tss ready for further use.
func ResetTimeSeries(tss []TimeSerie) []TimeSerie {
	clear(tss)
	return tss[:0]
}
