package protoparser

import (
	"fmt"
	"slices"
	"sync"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/dgryski/go-metro"
)

type TimeSerie struct {
	GroupLabels []Label
	Fingerprint uint64
}

type Label struct {
	Name  string
	Value string
}

func getWriteRequestUnmarshaler() *writeRequestUnmarshaler {
	v := wruPool.Get()
	if v == nil {
		return &writeRequestUnmarshaler{
			tss:        make([]TimeSerie, 0, 1024),
			labelsPool: make([]Label, 0, 4096),
			fpBuf:      make([]byte, 0, 4096),
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
	fpBuf      []byte
	labelsPool []Label
}

// Reset resets wru, so it could be re-used.
func (wru *writeRequestUnmarshaler) Reset() {
	wru.fpBuf = wru.fpBuf[:0]
	wru.tss = wru.tss[:0]
	wru.labelsPool = wru.labelsPool[:0]

}

func (wru *writeRequestUnmarshaler) UnmarshalProtobuf(src []byte, groupLabels []string, callback func(tss []TimeSerie)) error {
	wru.Reset()

	var err error

	tss := wru.tss

	// message WriteRequest {
	//    repeated TimeSeries timeseries = 1;
	//    reserved 2;
	//    repeated Metadata metadata = 3;
	// }
	labelsPool := wru.labelsPool
	fpBuf := wru.fpBuf
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
			labelsPool, fpBuf, err = ts.unmarshalProtobuf(data, groupLabels, labelsPool, fpBuf)
			if err != nil {
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
	wru.fpBuf = fpBuf
	return nil
}

func (ts *TimeSerie) unmarshalProtobuf(src []byte, groupLabels []string, labelsPool []Label, fpBuf []byte) ([]Label, []byte, error) {
	// message TimeSeries {
	//   repeated Label labels   = 1;
	//   repeated Sample samples = 2;
	// }
	fpBuf = fpBuf[:0]

	labelsPoolLen := len(labelsPool)
	var fc easyproto.FieldContext
	for len(src) > 0 {
		var err error
		src, err = fc.NextField(src)
		if err != nil {
			return labelsPool, fpBuf, fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return labelsPool, fpBuf, fmt.Errorf("cannot read label data")
			}
			if len(labelsPool) < cap(labelsPool) {
				labelsPool = labelsPool[:len(labelsPool)+1]
			} else {
				labelsPool = append(labelsPool, Label{})
			}
			label := &labelsPool[len(labelsPool)-1]
			if err := label.unmarshalProtobuf(data); err != nil {
				return labelsPool, fpBuf, fmt.Errorf("cannot unmarshal label: %w", err)
			}

			fpBuf = append(fpBuf, label.Name...)
			fpBuf = append(fpBuf, 0x00)
			fpBuf = append(fpBuf, label.Value...)
			fpBuf = append(fpBuf, 0x00)

			if !slices.Contains(groupLabels, label.Name) {
				labelsPool = labelsPool[:len(labelsPool)-1]
			}
		}
	}
	ts.GroupLabels = labelsPool[labelsPoolLen:]
	ts.Fingerprint = fingerprint(fpBuf)
	return labelsPool, fpBuf, nil
}

func (lbl *Label) unmarshalProtobuf(src []byte) (err error) {
	// message Label {
	//   string name  = 1;
	//   string value = 2;
	// }
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read the next field: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			name, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read label name")
			}
			lbl.Name = name
		case 2:
			value, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read label value")
			}
			lbl.Value = value
		}
	}
	return nil
}

func fingerprint(v []byte) uint64 {
	return metro.Hash64(v, 1337)
}
