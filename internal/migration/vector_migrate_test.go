package migration

import (
	"math"
	"testing"
)

func TestParseVectorMigrateArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantAddr  string
		wantBatch int
		wantDry   bool
		wantErr   bool
	}{
		{
			name:      "basic with addr",
			args:      []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334"},
			wantAddr:  "localhost:6334",
			wantBatch: 1000,
			wantDry:   false,
		},
		{
			name:      "custom batch size",
			args:      []string{"nram", "migrate-vectors", "--qdrant-addr", "qdrant:6334", "--batch-size", "500"},
			wantAddr:  "qdrant:6334",
			wantBatch: 500,
			wantDry:   false,
		},
		{
			name:      "dry run",
			args:      []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--dry-run"},
			wantAddr:  "localhost:6334",
			wantBatch: 1000,
			wantDry:   true,
		},
		{
			name:      "all flags",
			args:      []string{"nram", "migrate-vectors", "--qdrant-addr", "host:6334", "--batch-size", "200", "--dry-run"},
			wantAddr:  "host:6334",
			wantBatch: 200,
			wantDry:   true,
		},
		{
			name:    "missing addr",
			args:    []string{"nram", "migrate-vectors"},
			wantErr: true,
		},
		{
			name:    "addr without value",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr"},
			wantErr: true,
		},
		{
			name:    "batch-size without value",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--batch-size"},
			wantErr: true,
		},
		{
			name:    "invalid batch-size",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--batch-size", "abc"},
			wantErr: true,
		},
		{
			name:    "zero batch-size",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--batch-size", "0"},
			wantErr: true,
		},
		{
			name:    "negative batch-size",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--batch-size", "-5"},
			wantErr: true,
		},
		{
			name:    "unknown flag",
			args:    []string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334", "--verbose"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseVectorMigrateArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.QdrantAddr != tt.wantAddr {
				t.Errorf("QdrantAddr: got %q, want %q", got.QdrantAddr, tt.wantAddr)
			}
			if got.BatchSize != tt.wantBatch {
				t.Errorf("BatchSize: got %d, want %d", got.BatchSize, tt.wantBatch)
			}
			if got.DryRun != tt.wantDry {
				t.Errorf("DryRun: got %v, want %v", got.DryRun, tt.wantDry)
			}
		})
	}
}

func TestParseEmbeddingText(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []float32
		wantErr bool
	}{
		{
			name:  "empty embedding",
			input: "[]",
			want:  []float32{},
		},
		{
			name:  "single value",
			input: "[0.5]",
			want:  []float32{0.5},
		},
		{
			name:  "three values",
			input: "[0.1,0.2,0.3]",
			want:  []float32{0.1, 0.2, 0.3},
		},
		{
			name:  "negative values",
			input: "[-0.5,0.0,0.5]",
			want:  []float32{-0.5, 0.0, 0.5},
		},
		{
			name:  "values with spaces",
			input: "[0.1, 0.2, 0.3]",
			want:  []float32{0.1, 0.2, 0.3},
		},
		{
			name:  "leading/trailing whitespace",
			input: "  [1.0,2.0]  ",
			want:  []float32{1.0, 2.0},
		},
		{
			name:  "scientific notation",
			input: "[1.5e-3,2.0e2]",
			want:  []float32{1.5e-3, 200.0},
		},
		{
			name:    "missing brackets",
			input:   "0.1,0.2",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid float",
			input:   "[0.1,abc,0.3]",
			wantErr: true,
		},
		{
			name:    "only opening bracket",
			input:   "[",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseEmbeddingText(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("length mismatch: got %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if math.Abs(float64(got[i]-tt.want[i])) > 1e-6 {
					t.Errorf("index %d: got %f, want %f", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseVectorMigrateReverseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantAddr  string
		wantBatch int
		wantDry   bool
		wantErr   bool
	}{
		{
			name:      "basic with addr",
			args:      []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334"},
			wantAddr:  "localhost:6334",
			wantBatch: 1000,
			wantDry:   false,
		},
		{
			name:      "custom batch size",
			args:      []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "qdrant:6334", "--batch-size", "500"},
			wantAddr:  "qdrant:6334",
			wantBatch: 500,
			wantDry:   false,
		},
		{
			name:      "dry run",
			args:      []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--dry-run"},
			wantAddr:  "localhost:6334",
			wantBatch: 1000,
			wantDry:   true,
		},
		{
			name:      "all flags",
			args:      []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "host:6334", "--batch-size", "200", "--dry-run"},
			wantAddr:  "host:6334",
			wantBatch: 200,
			wantDry:   true,
		},
		{
			name:    "missing addr",
			args:    []string{"nram", "migrate-vectors-reverse"},
			wantErr: true,
		},
		{
			name:    "addr without value",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr"},
			wantErr: true,
		},
		{
			name:    "batch-size without value",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--batch-size"},
			wantErr: true,
		},
		{
			name:    "invalid batch-size",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--batch-size", "abc"},
			wantErr: true,
		},
		{
			name:    "zero batch-size",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--batch-size", "0"},
			wantErr: true,
		},
		{
			name:    "negative batch-size",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--batch-size", "-5"},
			wantErr: true,
		},
		{
			name:    "unknown flag",
			args:    []string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334", "--verbose"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseVectorMigrateReverseArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.QdrantAddr != tt.wantAddr {
				t.Errorf("QdrantAddr: got %q, want %q", got.QdrantAddr, tt.wantAddr)
			}
			if got.BatchSize != tt.wantBatch {
				t.Errorf("BatchSize: got %d, want %d", got.BatchSize, tt.wantBatch)
			}
			if got.DryRun != tt.wantDry {
				t.Errorf("DryRun: got %v, want %v", got.DryRun, tt.wantDry)
			}
		})
	}
}

func TestFormatEmbeddingText(t *testing.T) {
	tests := []struct {
		name  string
		input []float32
		want  string
	}{
		{
			name:  "empty embedding",
			input: []float32{},
			want:  "[]",
		},
		{
			name:  "single value",
			input: []float32{0.5},
			want:  "[0.5]",
		},
		{
			name:  "three values",
			input: []float32{0.1, 0.2, 0.3},
			want:  "[0.1,0.2,0.3]",
		},
		{
			name:  "negative values",
			input: []float32{-0.5, 0.0, 0.5},
			want:  "[-0.5,0,0.5]",
		},
		{
			name:  "integer-like values",
			input: []float32{1.0, 2.0, 3.0},
			want:  "[1,2,3]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatEmbeddingText(tt.input)
			if got != tt.want {
				t.Errorf("formatEmbeddingText(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatEmbeddingTextRoundTrip(t *testing.T) {
	// Verify that formatting and then parsing yields the same values.
	original := []float32{0.1, 0.2, 0.3, -0.5, 1.0}
	text := formatEmbeddingText(original)
	parsed, err := parseEmbeddingText(text)
	if err != nil {
		t.Fatalf("round-trip parse failed: %v", err)
	}
	if len(parsed) != len(original) {
		t.Fatalf("length mismatch: got %d, want %d", len(parsed), len(original))
	}
	for i := range original {
		if math.Abs(float64(parsed[i]-original[i])) > 1e-6 {
			t.Errorf("index %d: got %f, want %f", i, parsed[i], original[i])
		}
	}
}

func TestParseMigrateArgsMigrateVectorsReverse(t *testing.T) {
	cmd, extra, err := ParseMigrateArgs([]string{"nram", "migrate-vectors-reverse", "--qdrant-addr", "localhost:6334"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd != "migrate-vectors-reverse" {
		t.Errorf("command: got %q, want %q", cmd, "migrate-vectors-reverse")
	}
	if extra != "localhost:6334" {
		t.Errorf("extra: got %q, want %q", extra, "localhost:6334")
	}
}

func TestParseMigrateArgsMigrateVectors(t *testing.T) {
	cmd, extra, err := ParseMigrateArgs([]string{"nram", "migrate-vectors", "--qdrant-addr", "localhost:6334"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cmd != "migrate-vectors" {
		t.Errorf("command: got %q, want %q", cmd, "migrate-vectors")
	}
	if extra != "localhost:6334" {
		t.Errorf("extra: got %q, want %q", extra, "localhost:6334")
	}
}
