package model

import (
	"strings"
	"unicode"
)

// EntityTypeOther is the escape-hatch type for an extracted entity whose type
// does not fall in the closed vocabulary. It is a real, queryable type (not a
// sentinel) so off-vocabulary pressure stays measurable.
const EntityTypeOther = "other"

// EntityTypeUnknown is the internal stub sentinel: a placeholder entity created
// when a relationship references a name that has not been extracted yet. It is
// NOT part of the closed vocabulary and is never produced by coercing LLM
// output; CanonicalEntityType passes it through untouched so EntityRepo.Upsert's
// promoteStub path keeps working.
const EntityTypeUnknown = "unknown"

// CanonicalEntityTypes is the closed vocabulary of entity types. The extraction
// prompt instructs the model to choose from this set; CanonicalEntityType is the
// hard guarantee, coercing synonyms in and dropping everything else to "other".
var CanonicalEntityTypes = []string{
	// general
	"person", "organization", "location", "product", "event", "role", "date",
	"concept",
	// software / technical
	"technology", "software", "code_symbol", "file", "data_store", "system",
	"configuration", "command", "vcs_ref", "credential", "identifier", "metric",
	// knowledge / content
	"document", "research_artifact",
	// medical
	"medication", "medical_condition", "biomarker",
	// escape hatch
	EntityTypeOther,
}

// entityTypeAliases maps a canonical type to the off-vocabulary strings that
// collapse onto it. Keys are matched after normalization (see normalizeLabel),
// so "database_table", "database table", and "Database  Table" all resolve via
// the single normalized key "database table". The long tail of genuinely
// ambiguous singletons is intentionally left unmapped so it falls to "other".
var entityTypeAliases = map[string][]string{
	"person":            {"user", "author", "individual", "people"},
	"organization":      {"company", "team", "department", "vendor", "committee", "division", "institution", "group", "organization_handle", "enterprise_type", "industry", "industry_sector", "agency"},
	"location":          {"place", "area", "region", "location_type", "facility", "address", "geography", "site"},
	"product":           {"brand", "device", "device_category", "equipment", "material", "asset", "appliance", "gadget"},
	"event":             {"milestone", "incident", "activity", "award", "occasion", "ceremony"},
	"role":              {"occupation", "profession", "title", "position", "job", "responsibility"},
	"date":              {"time", "temporal", "duration", "timeframe", "time_interval", "time_range", "time_period", "time_duration", "period", "timestamp", "datetime"},
	"concept":           {"idea", "topic", "theme", "category", "subcategory", "principle", "heuristic", "pattern", "design_pattern", "mechanism", "capability", "functionality", "functional_area", "application_focus", "benefit", "signal", "context", "scope", "solution", "approach", "comparison", "trait", "subtrait", "legal_concept", "law", "regulation", "policy", "search_method", "strategy", "goal", "objective", "value_proposition"},
	"technology":        {"platform", "protocol", "standard", "language", "programming_language", "framework", "technology_stack", "methodology", "architecture", "design", "specification", "algorithm", "format", "data_format", "file_format", "transport", "middleware", "model", "os", "operating_system", "environment", "paradigm"},
	"software":          {"application", "program", "service", "service_name", "service_category", "tool", "utility", "library", "library_module", "package", "code_package", "module", "submodule", "software_module", "software_component", "plugin", "crate", "binary", "bin", "container", "scheduler", "daemon", "templating_engine", "app"},
	"code_symbol":       {"function", "function_call", "method", "class", "interface", "variable", "constant", "struct", "enum", "endpoint", "api", "hook", "code", "code_macro", "docstring", "instruction", "parameter", "property", "field", "data_field", "object", "data_type", "header", "http_header", "http_header_value", "status_code", "error", "method_call", "decorator", "annotation"},
	"file":              {"code_file", "file_path", "file_name", "file_reference", "file_type", "file_extension", "directory", "path", "fileset", "page", "document_section", "section", "line_reference", "folder"},
	"data_store":        {"database", "database_table", "table", "column", "schema", "data_structure", "dataset", "data_set", "data_source", "data_store", "data_volume", "database_field", "database_technology", "data", "data_stream", "cache", "query", "data_repository", "index", "collection"},
	"system":            {"component", "system_component", "technical_component", "server", "hardware", "network", "infrastructure", "machine", "port", "device_function", "node", "cluster", "host"},
	"configuration":     {"config", "setting", "settings", "flag", "environment_variable", "option", "system_configuration", "security_setting", "test_configuration", "feature_flag", "toggle"},
	"command":           {"script", "sql_command", "command_line_argument", "cli", "shell_command", "subcommand"},
	"vcs_ref":           {"commit", "branch", "version", "software_version", "release", "repository", "git_repository", "code_repository", "migration", "release_stage", "tag", "pull_request", "merge_request"},
	"credential":        {"credentials", "password", "secret", "token", "api_key", "credential_constant", "credential_type", "key", "auth_check", "certificate"},
	"identifier":        {"url", "website", "email", "email_address", "ip_address", "domain", "phone", "phone_number", "handle", "identifier", "document_identifier", "publication_id", "request_id", "account", "session", "uri", "uuid", "username"},
	"metric":            {"measurement", "quantity", "number", "numerical", "value", "money", "financial", "financial_metric", "financial_instrument", "financial_offer", "currency", "currency_amount", "percentage", "rate", "speed", "frequency", "frequency_band", "memory_size", "dimension", "weight", "safety_metric", "bug_count", "document_count", "count", "ratio", "score", "threshold"},
	"document":          {"report", "book", "journal", "review", "survey", "contract", "license", "certification", "plan", "plan_reference", "design_specification", "article", "manual", "guide", "spec_document", "memo"},
	"research_artifact": {"study", "study_type", "study_phase", "clinical_trial", "clinical_trial_phase", "trial", "trial_phase", "benchmark", "paper", "publication", "experiment", "population", "outcome", "case", "study_area", "cohort", "finding"},
	"medication":        {"drug", "drug_class", "drug_combination", "drug_dose", "dose", "treatment", "intervention", "therapy", "route", "regimen", "prescription"},
	"medical_condition": {"disease", "condition", "symptom", "side_effect", "adverse_event", "diagnosis", "cancer", "syndrome", "medical_outcome", "ailment", "disorder"},
	"biomarker":         {"gene", "genetic_marker", "receptor", "receptor_pair", "biological_structure", "biological_process", "biological_measure", "anatomical_site", "protein", "hormone", "enzyme"},
}

// entityTypeLookup is the precomputed normalized-synonym -> canonical map,
// including each canonical token's own normalized form so already-canonical
// input round-trips.
var entityTypeLookup = buildEntityTypeLookup()

func buildEntityTypeLookup() map[string]string {
	m := make(map[string]string, 512)
	for _, c := range CanonicalEntityTypes {
		m[normalizeLabel(c)] = c
	}
	for canonical, aliases := range entityTypeAliases {
		for _, a := range aliases {
			m[normalizeLabel(a)] = canonical
		}
	}
	return m
}

// CanonicalEntityType coerces a raw, LLM-supplied entity type into the closed
// vocabulary. The "unknown" stub sentinel is preserved verbatim. An empty or
// off-vocabulary type collapses to "other". It is idempotent.
func CanonicalEntityType(raw string) string {
	n := normalizeLabel(raw)
	if n == "" {
		return EntityTypeOther
	}
	if n == EntityTypeUnknown {
		return EntityTypeUnknown
	}
	if c, ok := entityTypeLookup[n]; ok {
		return c
	}
	return EntityTypeOther
}

// normalizeLabel lowercases, trims, and collapses every run of underscore,
// hyphen, and Unicode whitespace into a single ASCII space, with no leading or
// trailing separator. Shared by the entity-type and relation vocabulary lookups
// so their keys normalize identically.
func normalizeLabel(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	prevSep := false
	for _, r := range s {
		if r == '_' || r == '-' || unicode.IsSpace(r) {
			if !prevSep && b.Len() > 0 {
				b.WriteByte(' ')
				prevSep = true
			}
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		prevSep = false
	}
	return strings.TrimRight(b.String(), " ")
}
