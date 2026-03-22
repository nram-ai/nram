package hnsw

import (
	"math"
	"math/rand"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	dims := []int{384, 768, 1536}
	rng := rand.New(rand.NewSource(42))

	for _, dim := range dims {
		v := make([]float32, dim)
		for i := range v {
			v[i] = rng.Float32()*2 - 1 // [-1, 1)
		}

		blob := EncodeVector(v)
		got, err := DecodeVector(blob)
		if err != nil {
			t.Fatalf("dim=%d: DecodeVector error: %v", dim, err)
		}
		if len(got) != dim {
			t.Fatalf("dim=%d: got len %d, want %d", dim, len(got), dim)
		}
		for i := range v {
			if got[i] != v[i] {
				t.Fatalf("dim=%d: index %d: got %v, want %v", dim, i, got[i], v[i])
			}
		}
	}
}

func TestEncodeEmpty(t *testing.T) {
	blob := EncodeVector([]float32{})
	if len(blob) != 0 {
		t.Fatalf("expected empty bytes, got %d bytes", len(blob))
	}

	blob = EncodeVector(nil)
	if len(blob) != 0 {
		t.Fatalf("expected empty bytes for nil, got %d bytes", len(blob))
	}
}

func TestDecodeEmpty(t *testing.T) {
	v, err := DecodeVector([]byte{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(v))
	}

	v, err = DecodeVector(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil: %v", err)
	}
	if len(v) != 0 {
		t.Fatalf("expected empty slice for nil, got len %d", len(v))
	}
}

func TestDecodeInvalidLength(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 7, 9, 15} {
		b := make([]byte, n)
		_, err := DecodeVector(b)
		if err == nil {
			t.Fatalf("expected error for length %d, got nil", n)
		}
	}
}

func TestEncodeDecodeSpecialValues(t *testing.T) {
	special := []float32{
		float32(math.NaN()),
		float32(math.Inf(1)),
		float32(math.Inf(-1)),
		0,
		float32(math.Copysign(0, -1)),
		math.SmallestNonzeroFloat32,
		math.MaxFloat32,
		-math.MaxFloat32,
		-math.SmallestNonzeroFloat32,
	}

	blob := EncodeVector(special)
	got, err := DecodeVector(blob)
	if err != nil {
		t.Fatalf("DecodeVector error: %v", err)
	}
	if len(got) != len(special) {
		t.Fatalf("got len %d, want %d", len(got), len(special))
	}

	for i := range special {
		// NaN != NaN, so compare bit patterns
		wantBits := math.Float32bits(special[i])
		gotBits := math.Float32bits(got[i])
		if wantBits != gotBits {
			t.Fatalf("index %d: got bits %08x, want %08x", i, gotBits, wantBits)
		}
	}
}

func TestEncodedSize(t *testing.T) {
	tests := []struct {
		dim      int
		wantSize int
	}{
		{384, 1536},
		{1536, 6144},
		{768, 3072},
		{1, 4},
		{0, 0},
	}

	for _, tt := range tests {
		v := make([]float32, tt.dim)
		blob := EncodeVector(v)
		if len(blob) != tt.wantSize {
			t.Fatalf("dim=%d: got %d bytes, want %d", tt.dim, len(blob), tt.wantSize)
		}
	}
}
