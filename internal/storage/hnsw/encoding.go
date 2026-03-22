package hnsw

import (
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeVector encodes a float32 slice as a little-endian binary BLOB.
// Each float32 occupies 4 bytes. A 384-dim vector becomes 1,536 bytes.
func EncodeVector(v []float32) []byte {
	if len(v) == 0 {
		return []byte{}
	}
	buf := make([]byte, len(v)*4)
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

// DecodeVector decodes a little-endian binary BLOB back into a float32 slice.
// Returns an error if the byte length is not a multiple of 4.
func DecodeVector(b []byte) ([]float32, error) {
	if len(b) == 0 {
		return []float32{}, nil
	}
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("invalid blob length %d: must be a multiple of 4", len(b))
	}
	n := len(b) / 4
	v := make([]float32, n)
	for i := range n {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
	}
	return v, nil
}
