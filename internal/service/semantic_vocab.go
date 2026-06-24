package service

import (
	"context"
	"strings"
	"sync"

	"github.com/nram-ai/nram/internal/model"
	"github.com/nram-ai/nram/internal/provider"
	"github.com/nram-ai/nram/internal/storage/hnsw"
)

// SemanticClassifier coerces a raw extraction label (relation or entity type)
// into a closed vocabulary. It tries the deterministic synonym map first and,
// for anything that falls to the catch-all (related_to / other), classifies the
// label by embedding it and taking the nearest canonical term by cosine
// similarity. This catches the open-ended, often-malformed verbs and types an
// 8b model emits ("managed_servers", "ran_sendmail_server") that a static map
// can never enumerate, keeping the meaningful-label rate high.
//
// Reference embeddings (one per canonical term, from a descriptive gloss) are
// built once on first use; per-label results are memoized. Safe for concurrent
// use by the worker pool.
type SemanticClassifier struct {
	embedder  func() provider.EmbeddingProvider
	fallback  string
	staticMap func(string) string
	glosses   map[string]string // canonical term -> descriptive gloss
	threshold float64

	mu       sync.RWMutex
	refs     map[string][]float32
	refNorms map[string]float32
	built    bool
	cache    map[string]string
}

func newSemanticClassifier(embedder func() provider.EmbeddingProvider, fallback string, staticMap func(string) string, glosses map[string]string, threshold float64) *SemanticClassifier {
	return &SemanticClassifier{
		embedder:  embedder,
		fallback:  fallback,
		staticMap: staticMap,
		glosses:   glosses,
		threshold: threshold,
		cache:     make(map[string]string),
	}
}

// Classify returns the canonical term for raw: the static map result when it
// hits, otherwise the nearest canonical by embedding, otherwise the fallback.
func (c *SemanticClassifier) Classify(ctx context.Context, raw string) string {
	canon := c.staticMap(raw)
	if canon != c.fallback {
		return canon
	}
	return c.embedClassify(ctx, raw)
}

func (c *SemanticClassifier) embedClassify(ctx context.Context, raw string) string {
	if c == nil || c.embedder == nil {
		return c.fallback
	}
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		return c.fallback
	}
	c.mu.RLock()
	cached, ok := c.cache[key]
	c.mu.RUnlock()
	if ok {
		return cached
	}
	ep := c.embedder()
	if ep == nil {
		return c.fallback
	}
	if !c.ensureRefs(ctx, ep) {
		return c.fallback
	}
	resp, err := ep.Embed(provider.WithOperation(ctx, provider.OperationEmbedding), &provider.EmbeddingRequest{Input: []string{key}})
	if err != nil || len(resp.Embeddings) == 0 || len(resp.Embeddings[0]) == 0 {
		return c.fallback
	}
	vec := resp.Embeddings[0]
	vnorm := hnsw.Norm(vec)
	best := c.fallback
	bestSim := c.threshold
	c.mu.RLock()
	for term, ref := range c.refs {
		if len(ref) != len(vec) {
			continue
		}
		sim := float64(hnsw.CosineSimilarityWithNorms(vec, ref, vnorm, c.refNorms[term]))
		if sim >= bestSim {
			bestSim = sim
			best = term
		}
	}
	c.mu.RUnlock()
	c.mu.Lock()
	// Bound the memo: an 8b model's off-vocabulary label space is effectively
	// unbounded over a long-running worker, so cap the cache rather than let it
	// grow forever. Keeping the first N distinct labels (the common ones seen
	// early) cached is enough; rarer later labels just re-embed.
	if len(c.cache) < maxSemanticCacheEntries {
		c.cache[key] = best
	}
	c.mu.Unlock()
	return best
}

// maxSemanticCacheEntries caps the per-classifier memo of label -> canonical.
const maxSemanticCacheEntries = 8192

// ensureRefs builds the per-canonical reference embeddings once. Returns false
// when the embedder produced nothing usable (callers then degrade to fallback).
func (c *SemanticClassifier) ensureRefs(ctx context.Context, ep provider.EmbeddingProvider) bool {
	c.mu.RLock()
	built := c.built
	c.mu.RUnlock()
	if built {
		return len(c.refs) > 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.built {
		return len(c.refs) > 0
	}
	c.built = true // attempt once; a failure degrades to static-only thereafter

	terms := make([]string, 0, len(c.glosses))
	inputs := make([]string, 0, len(c.glosses))
	for term, gloss := range c.glosses {
		terms = append(terms, term)
		inputs = append(inputs, gloss)
	}
	resp, err := ep.Embed(provider.WithOperation(ctx, provider.OperationEmbedding), &provider.EmbeddingRequest{Input: inputs})
	if err != nil || len(resp.Embeddings) != len(terms) {
		return false
	}
	c.refs = make(map[string][]float32, len(terms))
	c.refNorms = make(map[string]float32, len(terms))
	for i, term := range terms {
		v := resp.Embeddings[i]
		if len(v) == 0 {
			continue
		}
		c.refs[term] = v
		c.refNorms[term] = hnsw.Norm(v)
	}
	return len(c.refs) > 0
}

// NewRelationClassifier builds a classifier over the closed relation vocabulary.
func NewRelationClassifier(embedder func() provider.EmbeddingProvider, threshold float64) *SemanticClassifier {
	return newSemanticClassifier(embedder, model.RelationRelatedTo, model.CanonicalRelationVocab, relationGlosses, threshold)
}

// NewEntityTypeClassifier builds a classifier over the closed entity-type
// vocabulary. The "unknown" stub sentinel is never produced by the static map
// for genuine LLM output, so only "other" triggers the embedding fallback.
func NewEntityTypeClassifier(embedder func() provider.EmbeddingProvider, threshold float64) *SemanticClassifier {
	return newSemanticClassifier(embedder, model.EntityTypeOther, model.CanonicalEntityType, entityTypeGlosses, threshold)
}

// ApplyRelation coerces rel.Relation via the classifier (static map + embedding
// fallback) and stamps kinship subtype, mirroring model.ApplyRelationVocab but
// with the semantic backstop. A nil classifier degrades to the static map.
func (c *SemanticClassifier) ApplyRelation(ctx context.Context, rel *model.Relationship) {
	if c == nil {
		model.ApplyRelationVocab(rel)
		return
	}
	kind := model.RelationKind(rel.Relation)
	canon := c.Classify(ctx, rel.Relation)
	rel.Relation = canon
	if kind != "" && canon == "family of" {
		rel.Properties = model.StampRelationKindProperty(rel.Properties, kind)
	}
}

// relationGlosses describes each canonical relation so the embedder can place a
// raw verb near the right one. Glosses pack several surface forms to widen the
// semantic target.
var relationGlosses = map[string]string{
	"is a":           "is a kind of, is a type of, is an instance of, is an example of, is categorized as",
	"part of":        "is part of, belongs to, is a component of, is contained within, is a subset of",
	"has part":       "contains, includes, is composed of, has as a component, encompasses, groups",
	"located in":     "is located in, is based in, lives in, resides in, is situated near a place or region",
	"uses":           "uses, calls, invokes, operates, is written in, is built with, consumes, deploys, runs a tool or technology",
	"depends on":     "depends on, requires, needs, relies on, is hosted by, is served by, runs on, is provided by",
	"produces":       "creates, authors, builds, founds, develops, makes, generates, writes, designs, establishes",
	"affects":        "manages, leads, oversees, administers, operates, modifies, influences, controls, changes, drives",
	"interacts with": "communicates with, collaborates with, works with, connects to, talks to, spends time with",
	"references":     "references, mentions, cites, describes, teaches, documents, discusses, points to a topic",
	"implements":     "implements, defines, realizes, satisfies, conforms to, adheres to a specification",
	"supports":       "supports, provides, serves, hosts, enables, powers, facilitates, offers a capability",
	"compares to":    "is compared to, is similar to, is a competitor of, differs from, is an alternative to, contrasts with",
	"member of":      "works at, is employed by, is a member of, studied at, is affiliated with an organization, joined",
	"family of":      "is married to, is the parent of, is the child of, is a sibling of, is a relative of, kinship",
	"has property":   "has a trait, has a property, holds a title, earned a credential, has a role, is characterized by an attribute",
}

// entityTypeGlosses describes each canonical entity type for the embedding
// fallback. "other" and "unknown" are intentionally absent (they are the
// fallback / stub sentinel, never classification targets).
var entityTypeGlosses = map[string]string{
	"person":            "a person, individual, or human referred to by name",
	"organization":      "a company, organization, business, team, agency, or institution",
	"location":          "a place, location, city, country, region, or geographic area",
	"product":           "a physical product, device, appliance, or piece of equipment",
	"event":             "an event, incident, milestone, ceremony, or occasion",
	"role":              "a job role, occupation, profession, position, or title held by a person",
	"date":              "a date, time, duration, or temporal period",
	"concept":           "an abstract concept, idea, topic, principle, theme, or category",
	"technology":        "a technology, programming language, protocol, standard, framework, or platform",
	"software":          "a software application, tool, library, package, module, service, or program",
	"code_symbol":       "a code symbol such as a function, method, class, variable, struct, API endpoint, or interface",
	"file":              "a file, directory, file path, or document stored on disk",
	"data_store":        "a database, table, schema, dataset, cache, or data store",
	"system":            "a system, server, hardware component, device, network, or piece of infrastructure",
	"configuration":     "a configuration setting, flag, parameter, option, or environment variable",
	"command":           "a command, script, or command-line invocation",
	"vcs_ref":           "a version control reference such as a commit, branch, version, release, or repository",
	"credential":        "a credential, password, secret, token, API key, or certificate",
	"identifier":        "an identifier such as a URL, email address, IP address, domain, phone number, or handle",
	"metric":            "a metric, measurement, quantity, number, amount, percentage, or financial figure",
	"document":          "a document, report, book, article, manual, or publication",
	"research_artifact": "a research artifact such as a study, clinical trial, benchmark, paper, or dataset",
	"medication":        "a medication, drug, dose, therapy, or treatment",
	"medical_condition": "a medical condition, disease, symptom, syndrome, or diagnosis",
	"biomarker":         "a biomarker, gene, receptor, protein, hormone, or biological structure",
}
