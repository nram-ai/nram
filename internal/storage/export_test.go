package storage

// ScanCount exposes the facet brute-force scan counter to the external test
// package so tests can observe the facet-scan short-circuit (see scanFacetCandidates).
func (s *HNSWStore) ScanCount() int64 { return s.scanCount.Load() }
