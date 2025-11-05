package gostry

import (
	"context"
)

// metaKey is an unexported context key type.
type metaKey struct{}
type skipKey struct{}

// WithOperator attaches an operator identifier to the context.
func WithOperator(ctx context.Context, v string) context.Context {
	m := extractMeta(ctx)
	m.operator = v
	return context.WithValue(ctx, metaKey{}, m)
}

// WithTraceID attaches a trace identifier.
func WithTraceID(ctx context.Context, v string) context.Context {
	m := extractMeta(ctx)
	m.traceID = v
	return context.WithValue(ctx, metaKey{}, m)
}

// WithReason attaches a human-readable reason for the operation.
func WithReason(ctx context.Context, v string) context.Context {
	m := extractMeta(ctx)
	m.reason = v
	return context.WithValue(ctx, metaKey{}, m)
}

// WithSkip marks the context so gostry bypasses capture for subsequent statements.
func WithSkip(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipKey{}, true)
}

// extractMeta extracts metadata from context.
func extractMeta(ctx context.Context) meta {
	if v := ctx.Value(metaKey{}); v != nil {
		if m, ok := v.(meta); ok {
			return m
		}
	}
	return meta{}
}

// extractSkip extracts skip flag from context.
func extractSkip(ctx context.Context) bool {
	if v, ok := ctx.Value(skipKey{}).(bool); ok {
		return v
	}
	return false
}
