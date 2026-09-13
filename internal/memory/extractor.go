package memory

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// FactExtractor analyzes task actions, outputs, and summaries to extract
// durable architectural facts, decisions, and conventions.
type FactExtractor struct {
	bulletRegex *regexp.Regexp
	prefixRegex *regexp.Regexp
}

// NewFactExtractor creates an initialized FactExtractor.
func NewFactExtractor() *FactExtractor {
	return &FactExtractor{
		// Matches: - **[topic]**: content, - [topic]: content, [topic]: content, - **Topic**: content
		bulletRegex: regexp.MustCompile(`^(?:[-*]\s+)?(?:\*\*\[([^\]]+)\]\*\*|\[([^\]]+)\]|\*\*([^*:]+)\*\*):?\s*(.+)$`),
		// Matches: Decision: ..., Convention: ..., Architecture: ..., etc.
		prefixRegex: regexp.MustCompile(`^(?:[-*]\s+)?(?i)(decision|convention|architecture|database|storage|security|pattern|rule|fact|convention):\s*(.+)$`),
	}
}

// Extract processes multiple task outputs from a session and returns deduplicated durable facts.
func (e *FactExtractor) Extract(sessionID string, taskOutputs map[string]string) []Fact {
	if len(taskOutputs) == 0 {
		return []Fact{}
	}

	seen := make(map[string]bool)
	var facts []Fact

	for key, output := range taskOutputs {
		extracted := e.ExtractFromTask(sessionID, key, output)
		for _, f := range extracted {
			dedupKey := strings.ToLower(f.Topic) + "::" + strings.ToLower(strings.TrimSpace(f.Content))
			if !seen[dedupKey] {
				seen[dedupKey] = true
				facts = append(facts, f)
			}
		}
	}

	return facts
}

// ExtractFromTask analyzes a single task output using its action/key and payload.
func (e *FactExtractor) ExtractFromTask(sessionID string, actionOrKey string, output string) []Fact {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return nil
	}

	var facts []Fact

	// 1. Try parsing structured JSON
	if factsFromJSON := e.extractFromJSON(sessionID, actionOrKey, trimmed); len(factsFromJSON) > 0 {
		facts = append(facts, factsFromJSON...)
	}

	// 2. Try parsing plain text / markdown
	factsFromText := e.ExtractFromText(sessionID, inferTopicFromKey(actionOrKey), trimmed)
	facts = append(facts, factsFromText...)

	return deduplicateFacts(facts)
}

func (e *FactExtractor) extractFromJSON(sessionID string, actionOrKey string, rawJSON string) []Fact {
	var parsed any
	if err := json.Unmarshal([]byte(rawJSON), &parsed); err != nil {
		return nil
	}

	obj, ok := parsed.(map[string]any)
	if !ok {
		return nil
	}

	var facts []Fact
	now := time.Now().UTC()

	// A. Check explicit "facts" array
	if rawFacts, ok := obj["facts"]; ok {
		if list, ok := rawFacts.([]any); ok {
			for _, item := range list {
				if fMap, ok := item.(map[string]any); ok {
					content, _ := fMap["content"].(string)
					topic, _ := fMap["topic"].(string)
					scope, _ := fMap["scope"].(string)
					if content != "" {
						if topic == "" {
							topic = inferTopicFromKey(actionOrKey)
						}
						if scope == "" {
							scope = "project"
						}
						facts = append(facts, Fact{
							ID:        generateFactID(topic, content),
							SessionID: sessionID,
							Topic:     cleanTopic(topic),
							Content:   strings.TrimSpace(content),
							Scope:     scope,
							CreatedAt: now,
							UpdatedAt: now,
						})
					}
				} else if str, ok := item.(string); ok && str != "" {
					extracted := e.ExtractFromText(sessionID, inferTopicFromKey(actionOrKey), str)
					facts = append(facts, extracted...)
				}
			}
		}
	}

	// B. Check architectural / convention keys
	knownKeys := []string{
		"architecture",
		"decisions",
		"decision",
		"conventions",
		"convention",
		"database",
		"rules",
		"storage",
		"security",
	}

	for _, k := range knownKeys {
		val, exists := obj[k]
		if !exists {
			continue
		}
		topic := cleanTopic(k)
		if strings.HasSuffix(topic, "s") && topic != "rules" && topic != "conventions" {
			topic = strings.TrimSuffix(topic, "s")
		}

		switch v := val.(type) {
		case string:
			if strings.TrimSpace(v) != "" {
				facts = append(facts, Fact{
					ID:        generateFactID(topic, v),
					SessionID: sessionID,
					Topic:     topic,
					Content:   strings.TrimSpace(v),
					Scope:     "project",
					CreatedAt: now,
					UpdatedAt: now,
				})
			}
		case []any:
			for _, item := range v {
				if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
					facts = append(facts, Fact{
						ID:        generateFactID(topic, str),
						SessionID: sessionID,
						Topic:     topic,
						Content:   strings.TrimSpace(str),
						Scope:     "project",
						CreatedAt: now,
						UpdatedAt: now,
					})
				}
			}
		}
	}

	// C. If "findings" or "summary" string is present, inspect text
	for _, summaryKey := range []string{"findings", "summary", "notes"} {
		if val, ok := obj[summaryKey]; ok {
			if str, ok := val.(string); ok {
				facts = append(facts, e.ExtractFromText(sessionID, inferTopicFromKey(actionOrKey), str)...)
			}
		}
	}

	return facts
}

// ExtractFromText scans text line-by-line for structured conventions, decisions, and architectural facts.
func (e *FactExtractor) ExtractFromText(sessionID string, defaultTopic string, text string) []Fact {
	lines := strings.Split(text, "\n")
	var facts []Fact
	now := time.Now().UTC()

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}

		// Check prefix regex: Decision: ..., Convention: ..., etc.
		if matches := e.prefixRegex.FindStringSubmatch(line); len(matches) == 3 {
			topic := cleanTopic(matches[1])
			content := strings.TrimSpace(matches[2])
			if content != "" {
				facts = append(facts, Fact{
					ID:        generateFactID(topic, content),
					SessionID: sessionID,
					Topic:     topic,
					Content:   content,
					Scope:     "project",
					CreatedAt: now,
					UpdatedAt: now,
				})
				continue
			}
		}

		// Check bullet regex: - **[topic]**: content or - [topic]: content
		if matches := e.bulletRegex.FindStringSubmatch(line); len(matches) == 5 {
			var topic string
			for i := 1; i <= 3; i++ {
				if matches[i] != "" {
					topic = cleanTopic(matches[i])
					break
				}
			}
			content := strings.TrimSpace(matches[4])
			if topic != "" && content != "" {
				facts = append(facts, Fact{
					ID:        generateFactID(topic, content),
					SessionID: sessionID,
					Topic:     topic,
					Content:   content,
					Scope:     "project",
					CreatedAt: now,
					UpdatedAt: now,
				})
				continue
			}
		}
	}

	return deduplicateFacts(facts)
}

func inferTopicFromKey(key string) string {
	lower := strings.ToLower(key)
	switch {
	case strings.Contains(lower, "db") || strings.Contains(lower, "database") || strings.Contains(lower, "sql"):
		return "database"
	case strings.Contains(lower, "arch") || strings.Contains(lower, "structure"):
		return "architecture"
	case strings.Contains(lower, "conv") || strings.Contains(lower, "rule"):
		return "convention"
	case strings.Contains(lower, "sec") || strings.Contains(lower, "auth"):
		return "security"
	case strings.Contains(lower, "decis"):
		return "decision"
	case strings.Contains(lower, "test"):
		return "testing"
	case strings.Contains(lower, "store") || strings.Contains(lower, "blackboard"):
		return "storage"
	default:
		return "general"
	}
}

func cleanTopic(t string) string {
	cleaned := strings.ToLower(strings.TrimSpace(t))
	cleaned = strings.TrimPrefix(cleaned, "[")
	cleaned = strings.TrimSuffix(cleaned, "]")
	switch cleaned {
	case "decisions":
		return "decision"
	case "conventions":
		return "convention"
	case "rules":
		return "rule"
	default:
		return cleaned
	}
}

func deduplicateFacts(facts []Fact) []Fact {
	if len(facts) <= 1 {
		return facts
	}
	seen := make(map[string]bool)
	var unique []Fact
	for _, f := range facts {
		k := strings.ToLower(f.Topic) + "::" + strings.ToLower(strings.TrimSpace(f.Content))
		if !seen[k] {
			seen[k] = true
			unique = append(unique, f)
		}
	}
	return unique
}
