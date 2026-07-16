package provider

import (
	"context"
	"errors"
	"fmt"
)

// Known-answer fixture for judge calibration. The pair is deliberately trivial
// and unambiguous: any model capable of acting as a relevance judge must score
// the relevant document above the irrelevant one. A judge that cannot separate
// these two is not going to separate real candidates, so the fixture tests the
// configuration rather than the model's discernment.
const (
	judgeCalibrationQuery      = "What is the capital of France?"
	judgeCalibrationRelevant   = "Paris is the capital and largest city of France."
	judgeCalibrationIrrelevant = "The Pacific Ocean is the largest ocean on Earth."
)

// judgeNotGenerativeDiagnosis is the operator-facing verdict when no candidate
// produced a number at all, which is the signature of a model that is not a
// generative judge rather than one that is merely misconfigured.
const judgeNotGenerativeDiagnosis = "the model never emitted a relevance number. It may be a cross-encoder " +
	"rather than a generative judge: point the slot at a real /v1/rerank endpoint " +
	"and let the probe detect cross_encoder, or use a chat model that can follow " +
	"the judge prompt. If it is a reasoning model, its thinking pass cannot be " +
	"disabled on this provider type and does not fit the token cap."

// JudgeCalibrationCandidate is one (thinking, token cap) configuration to try
// against the fixture.
type JudgeCalibrationCandidate struct {
	// DisableThinking is the value stamped onto the slot for this attempt. It
	// only reaches the wire for provider types that carry a thinking knob; for
	// the others the attempt runs at the model's own default.
	DisableThinking bool
	// MaxTokens is the per-candidate completion cap for this attempt.
	MaxTokens int
}

// JudgeCalibrationResult reports what the sweep found.
type JudgeCalibrationResult struct {
	// Calibrated reports whether some candidate produced parseable, discriminating
	// scores. When false, Diagnosis explains what went wrong and Winner is unset.
	Calibrated bool
	// Winner is the first candidate that worked. Only meaningful when Calibrated.
	Winner JudgeCalibrationCandidate
	// RelevantScore and IrrelevantScore are the winning candidate's fixture
	// scores, kept so the operator can see the separation rather than trust a
	// green check.
	RelevantScore   float64
	IrrelevantScore float64
	// Diagnosis is an operator-facing explanation when Calibrated is false.
	Diagnosis string
	// LastOutput is the raw completion behind a non-numeric failure, echoed so the
	// operator can see what the model actually said.
	LastOutput string
}

// CalibrateJudge drives the real judge path against a fixed known-answer pair,
// trying each candidate in order and returning the first that yields parseable
// scores which discriminate (relevant strictly above irrelevant).
//
// It exists because ProbeRerankMethod only establishes that a server is not a
// cross-encoder: a "judge" verdict means POST /v1/rerank 4xx'd, and the chat
// model is never invoked. Whether that model actually emits a usable relevance
// number depends on configuration that fails silently, so the only honest test is
// to run it.
//
// The provider package stays settings-agnostic (see RerankJudgeConfig): the
// caller resolves the prompt, temperature, and candidate ladder from settings and
// passes them in. A transport-level failure is returned as an error; a model that
// simply is not a judge comes back as a result with Calibrated false.
func CalibrateJudge(
	ctx context.Context,
	cfg SlotConfig,
	systemPrompt string,
	temperature float64,
	candidates []JudgeCalibrationCandidate,
) (*JudgeCalibrationResult, error) {
	return calibrateJudge(ctx, createRerankProvider, cfg, systemPrompt, temperature, candidates)
}

// calibrateJudge is CalibrateJudge with the provider construction injected, so a
// test can drive the real sweep (ladder order, early stop, diagnosis) against a
// stub model instead of a network.
func calibrateJudge(
	ctx context.Context,
	build func(SlotConfig) (RerankProvider, error),
	cfg SlotConfig,
	systemPrompt string,
	temperature float64,
	candidates []JudgeCalibrationCandidate,
) (*JudgeCalibrationResult, error) {
	if len(candidates) == 0 {
		return nil, errors.New("calibrate judge: no candidates")
	}

	res := &JudgeCalibrationResult{}
	sawNonNumeric := false

	for _, c := range candidates {
		attempt := cfg
		disable := c.DisableThinking
		attempt.DisableThinking = &disable
		attempt.RerankMethod = RerankMethodJudge

		rr, err := build(attempt)
		if err != nil {
			return nil, fmt.Errorf("calibrate judge: building provider: %w", err)
		}

		cctx := WithRerankJudgeConfig(ctx, RerankJudgeConfig{
			SystemPrompt: systemPrompt,
			MaxTokens:    c.MaxTokens,
			Temperature:  temperature,
		})
		resp, err := rr.Rerank(cctx, judgeCalibrationQuery, []string{
			judgeCalibrationRelevant,
			judgeCalibrationIrrelevant,
		})
		if err != nil {
			// A model that emitted no number is a configuration signal, not a
			// failure: record it and try the next rung.
			if noScore, ok := errors.AsType[*NoJudgeScoreError](err); ok {
				sawNonNumeric = true
				res.LastOutput = noScore.Content
				continue
			}
			return nil, fmt.Errorf("calibrate judge: %w", err)
		}
		if len(resp.Scores) != 2 {
			return nil, fmt.Errorf("calibrate judge: got %d scores, want 2", len(resp.Scores))
		}

		relevant, irrelevant := resp.Scores[0], resp.Scores[1]
		if relevant > irrelevant {
			res.Calibrated = true
			res.Winner = c
			res.RelevantScore = relevant
			res.IrrelevantScore = irrelevant
			return res, nil
		}
		// Parseable but flat: keep the scores so the failure is legible if no
		// later candidate wins.
		res.RelevantScore = relevant
		res.IrrelevantScore = irrelevant
	}

	if sawNonNumeric {
		// An earlier flat rung may have left scores on res, but they are not what
		// failed: the model never produced a number at all.
		res.Diagnosis = judgeNotGenerativeDiagnosis
		return res, nil
	}
	res.Diagnosis = fmt.Sprintf(
		"the model emitted numbers but did not discriminate (relevant %.3f vs irrelevant %.3f). "+
			"It scores every candidate alike, so it adds no ranking signal.",
		res.RelevantScore, res.IrrelevantScore,
	)
	return res, nil
}
