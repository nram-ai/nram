package metrics

import (
	"context"
	"time"

	"github.com/nram-ai/nram/internal/provider"
)

// instrumentedEmbeddingProvider wraps a provider.EmbeddingProvider so every
// embedding call increments nram_embeddings_total and records its duration
// into nram_embedding_duration_seconds. Failed calls are still recorded;
// duration covers the latency of both successes and failures, and the
// counter increments on attempt so a stuck provider is visible in the rate.
type instrumentedEmbeddingProvider struct {
	inner   provider.EmbeddingProvider
	metrics *Metrics
}

// WrapEmbeddingProvider returns ep wrapped with embedding metrics. Returns ep
// unchanged when m is nil so callers can wire metrics conditionally.
func WrapEmbeddingProvider(ep provider.EmbeddingProvider, m *Metrics) provider.EmbeddingProvider {
	if ep == nil || m == nil {
		return ep
	}
	return &instrumentedEmbeddingProvider{inner: ep, metrics: m}
}

func (p *instrumentedEmbeddingProvider) Embed(ctx context.Context, req *provider.EmbeddingRequest) (*provider.EmbeddingResponse, error) {
	start := time.Now()
	resp, err := p.inner.Embed(ctx, req)
	status := "success"
	if err != nil {
		status = "failure"
	}
	p.metrics.EmbeddingsTotal.WithLabelValues(status).Inc()
	p.metrics.EmbeddingDuration.Observe(time.Since(start).Seconds())
	return resp, err
}

func (p *instrumentedEmbeddingProvider) Name() string {
	return p.inner.Name()
}

func (p *instrumentedEmbeddingProvider) Dimensions() []int {
	return p.inner.Dimensions()
}
