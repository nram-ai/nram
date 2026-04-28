package provider

// Test-support helpers shared by package tests outside `provider`. Kept in
// a non-_test file so callers in other packages (service, enrichment,
// dreaming, server) can import them. They are thin factories that wrap a
// provider stub in the recording middleware so tests exercise the
// production wrap-order without spinning up a registry.

// WrapLLMForTest returns a function suitable for `func() LLMProvider` slots
// that wraps the given inner provider in UsageRecordingLLM whenever it is
// non-nil. nil → nil so test stubs can opt out per-call.
func WrapLLMForTest(inner func() LLMProvider, recorder UsageRecorder) func() LLMProvider {
	if inner == nil {
		return nil
	}
	return func() LLMProvider {
		lp := inner()
		if lp == nil {
			return nil
		}
		return NewUsageRecordingLLM(lp, recorder, nil)
	}
}

// WrapEmbeddingForTest mirrors WrapLLMForTest for embedding providers.
func WrapEmbeddingForTest(inner func() EmbeddingProvider, recorder UsageRecorder) func() EmbeddingProvider {
	if inner == nil {
		return nil
	}
	return func() EmbeddingProvider {
		ep := inner()
		if ep == nil {
			return nil
		}
		return NewUsageRecordingEmbedding(ep, recorder, nil)
	}
}
