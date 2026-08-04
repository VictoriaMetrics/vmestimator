package protoparser

import (
	"errors"
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/easyproto"
	"github.com/cespare/xxhash/v2"
)

var skipTs = errors.New("skip time series")

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

func getWriteRequestUnmarshaler() *writeRequestUnmarshaler {
	v := wruPool.Get()
	if v == nil {
		return &writeRequestUnmarshaler{
			tss:        make([]TimeSerie, 0, 1024),
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
	tss        []TimeSerie
	labelsPool []Label
}

// Reset resets wru, so it could be re-used.
func (wru *writeRequestUnmarshaler) Reset() {
	wru.tss = wru.tss[:0]
	wru.labelsPool = wru.labelsPool[:0]
}

func (wru *writeRequestUnmarshaler) UnmarshalProtobuf(src []byte, labelFP bool, callback func(tss []TimeSerie)) error {
	wru.Reset()

	var err error

	tss := wru.tss

	// message WriteRequest {
	//    repeated TimeSeries timeseries = 1;
	//    reserved 2;
	//    repeated Metadata metadata = 3;
	// }
	labelsPool := wru.labelsPool
	var fc easyproto.FieldContext
	for len(src) > 0 {
		if len(tss) >= cap(tss) {
			callback(tss)
			tss = tss[:0]
			labelsPool = labelsPool[:0]
		}

		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read timeseries data")
			}
			tss = tss[:len(tss)+1]
			ts := &tss[len(tss)-1]
			labelsPool, err = ts.unmarshalProtobuf(data, labelsPool, labelFP)
			if errors.Is(err, skipTs) {
				tss = tss[:len(tss)-1]
			} else if err != nil {
				return fmt.Errorf("cannot unmarshal timeseries: %w", err)
			}
		}
	}

	if len(tss) > 0 {
		callback(tss)
		tss = tss[:0]
		labelsPool = labelsPool[:0]
	}

	wru.tss = tss[:0]
	wru.labelsPool = labelsPool
	return nil
}

func (ts *TimeSerie) unmarshalProtobuf(src []byte, labelsPool []Label, labelFP bool) ([]Label, error) {
	// message TimeSeries {
	//   repeated Label labels   = 1;
	//   repeated Sample samples = 2;
	// }

	tsD := getDigest()
	defer putDigest(tsD)

	digestLabel := func(value []byte) uint64 {
		return 0
	}
	if labelFP {
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

			name := bytesutil.ToUnsafeString(nameBytes)
			value := bytesutil.ToUnsafeString(valueBytes)

			// Skip cardinality_estimate metrics — estimating the estimator's own output adds noise without value.
			// See https://github.com/VictoriaMetrics/vmestimator/issues/30
			if name == `__name__` && value == `cardinality_estimate` {
				return labelsPool[:labelsPoolLen], skipTs
			}

			_, _ = tsD.Write(data)
			labelsPool = append(labelsPool, Label{
				Name:        name,
				Value:       value,
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
