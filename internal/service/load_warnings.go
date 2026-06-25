package service

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
)

// providerLoadKnob is one configuration setting that can drive concurrent
// provider load when raised. The CheckProviderLoadDefaults helper compares
// the resolved value to SafeDefault and surfaces a startup warning when
// the operator has gone above the safe baseline. Why is included in the
// warning so the operator immediately understands what they're trading off.
type providerLoadKnob struct {
	Key         string
	SafeDefault int
	Why         string
}

// providerLoadKnobs is the canonical list of "could overload your provider"
// settings. Adding a knob here lights up the startup warning automatically;
// keep it in sync as new concurrency-shaped knobs land. The set is
// intentionally small so the warning stays actionable and is not noise.
var providerLoadKnobs = []providerLoadKnob{
	{
		Key:         SettingEnrichmentWorkerCountSQLite,
		SafeDefault: 1,
		Why:         "more workers = more concurrent LLM/embed calls; SQLite serializes writes anyway",
	},
	{
		Key:         SettingEnrichmentWorkerCountPostgres,
		SafeDefault: 1,
		Why:         "more workers = more concurrent LLM/embed calls; raise only when your provider can sustain parallel calls",
	},
	{
		Key:         SettingEnrichmentWorkerLLMConcurrency,
		SafeDefault: 1,
		Why:         "how many of a worker's claimed jobs run their LLM extraction calls in parallel within a batch; raise only with a multi-GPU or hosted provider",
	},
	{
		Key:         SettingEnrichmentWorkerBatchClaimSize,
		SafeDefault: 1,
		Why:         "larger batches mean larger per-batch embed calls and longer per-iteration latency spikes",
	},
	{
		Key:         SettingDreamContradictionNeighbors,
		SafeDefault: 1,
		Why:         "each unit increase multiplies the LLM-judge call volume per dream cycle",
	},
	{
		Key:         SettingDreamParaphraseTopK,
		SafeDefault: 1,
		Why:         "raises the per-anchor neighbor count in paraphrase dedup; check vector store + embed provider headroom",
	},
	{
		Key:         SettingDreamLLMConcurrency,
		SafeDefault: 1,
		Why:         "fans out each dream phase's per-item LLM/embedding calls; a cycle runs alone, so this is its whole provider concurrency. Raise only with a multi-GPU or hosted provider",
	},
	{
		Key:         SettingProviderLLMHostConcurrency,
		SafeDefault: 1,
		Why:         "max LLM completion requests in flight to one host across all slots and subsystems; this is the aggregate cap, so raise only when the model host can sustain that many parallel generations",
	},
	{
		Key:         SettingProviderEmbedHostConcurrency,
		SafeDefault: 1,
		Why:         "max embedding requests in flight to one host across all slots and subsystems; raise only when the embedder can sustain that many parallel calls",
	},
}

// CheckProviderLoadDefaults inspects every knob in providerLoadKnobs against
// its safe default and emits a single aggregated WARN log line listing those
// the operator has raised. Designed to run once at server start, after the
// settings cache is initialized, so an operator who has tuned these knobs
// for a beefy provider sees a one-time reminder of the trade-off, and an
// operator who hasn't tuned them sees nothing.
//
// The warning is intentionally a soft signal, not an error: there are
// legitimate reasons to raise these (multi-GPU host, hosted provider with
// large rate limits, batch backfill where throughput matters more than
// liveness). The goal is to make the trade-off visible so operators don't
// reach for "deadlock" diagnoses when the real cause is provider queueing.
func CheckProviderLoadDefaults(ctx context.Context, settings *SettingsService) {
	if settings == nil {
		return
	}

	type raised struct {
		key     string
		current int
		safe    int
		why     string
	}
	var hits []raised
	for _, knob := range providerLoadKnobs {
		current, err := settings.ResolveInt(ctx, knob.Key, "global")
		if err != nil {
			// No setting persisted means the default is in effect: nothing
			// to warn about. Other errors (db unreachable, malformed value)
			// are quiet here because the worker pool startup will fail
			// noisily on its own resolves moments later.
			continue
		}
		if current > knob.SafeDefault {
			hits = append(hits, raised{
				key:     knob.Key,
				current: current,
				safe:    knob.SafeDefault,
				why:     knob.Why,
			})
		}
	}
	if len(hits) == 0 {
		return
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].key < hits[j].key })

	var lines []string
	lines = append(lines,
		"provider-load knobs raised above the safe defaults: concurrent calls to a single-GPU local provider (Ollama, llama.cpp) can queue at the model level and look like deadlocks; if you see provider timeouts or apparent hangs, consider returning these to defaults via /admin/settings:")
	for _, h := range hits {
		lines = append(lines,
			fmt.Sprintf("  • %s = %s (default %s): %s",
				h.key, strconv.Itoa(h.current), strconv.Itoa(h.safe), h.why))
	}

	slog.Warn(strings.Join(lines, "\n"))
}
