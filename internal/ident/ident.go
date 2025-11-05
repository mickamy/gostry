package ident

import (
	"strings"
	"unicode"
)

// HistoryParts returns qualified identifier parts with suffix applied to the base table name.
func HistoryParts(base, suffix string) []string {
	parts := SplitQualified(base)
	if len(parts) == 0 {
		if suffix == "" {
			return nil
		}
		return []string{suffix}
	}
	out := make([]string, len(parts))
	copy(out, parts)
	out[len(out)-1] = out[len(out)-1] + suffix
	return out
}

// SplitQualified splits a potentially schema-qualified identifier into its parts.
func SplitQualified(ident string) []string {
	ident = strings.TrimSpace(ident)
	if ident == "" {
		return nil
	}
	var parts []string
	var buf strings.Builder
	inQuotes := false
	runes := []rune(ident)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '"':
			if inQuotes && i+1 < len(runes) && runes[i+1] == '"' {
				buf.WriteRune('"')
				i++
				continue
			}
			inQuotes = !inQuotes
		case '.':
			if inQuotes {
				buf.WriteRune(r)
				continue
			}
			part := strings.TrimSpace(buf.String())
			parts = append(parts, part)
			buf.Reset()
		default:
			buf.WriteRune(r)
		}
	}
	part := strings.TrimSpace(buf.String())
	parts = append(parts, part)
	return parts
}

// StripAlias removes trailing alias tokens from an identifier while preserving quotes.
func StripAlias(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, ",")
	runes := []rune(s)
	inQuotes := false
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '"':
			inQuotes = !inQuotes
		default:
			if !inQuotes && unicode.IsSpace(r) {
				return strings.TrimSpace(string(runes[:i]))
			}
		}
	}
	return s
}

// QuoteQualified renders qualified identifier parts as a SQL identifier.
func QuoteQualified(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = Quote(p)
	}
	return strings.Join(quoted, ".")
}

// Quote safely quotes a single identifier part.
func Quote(part string) string {
	return `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
}

// QualifiedRegclassLiteral produces a regclass literal (e.g. 'public.table') with proper quoting.
func QualifiedRegclassLiteral(parts []string) string {
	if len(parts) == 0 {
		return "''"
	}
	ident := QuoteQualified(parts)
	return "'" + strings.ReplaceAll(ident, "'", "''") + "'"
}

// BaseTableName returns the last segment of a qualified identifier.
func BaseTableName(ident string) string {
	parts := SplitQualified(ident)
	if len(parts) == 0 {
		return strings.TrimSpace(ident)
	}
	return parts[len(parts)-1]
}
