package gostry

import (
	"reflect"
	"testing"
)

func TestSplitQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   string
		want []string
	}{
		{name: "simple", in: "orders", want: []string{"orders"}},
		{name: "schema qualified", in: "public.orders", want: []string{"public", "orders"}},
		{name: "quoted schema and space", in: `"Sales"."Order Detail"`, want: []string{"Sales", "Order Detail"}},
		{name: "dot inside quotes", in: `"Sales"."Order.Detail"`, want: []string{"Sales", "Order.Detail"}},
		{name: "escaped quote", in: `"Sales""Region"."Orders"`, want: []string{`Sales"Region`, "Orders"}},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := splitQualifiedIdentifier(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("splitQualifiedIdentifier(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestHistoryIdentifierParts(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name   string
		base   string
		suffix string
		want   []string
	}{
		{name: "schema qualified", base: "public.orders", suffix: "_history", want: []string{"public", "orders_history"}},
		{name: "simple table", base: "orders", suffix: "_history", want: []string{"orders_history"}},
		{name: "empty base", base: "", suffix: "_history", want: []string{"_history"}},
		{name: "quoted", base: `"Sales"."Orders"`, suffix: "_history", want: []string{"Sales", "Orders_history"}},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := historyIdentifierParts(tc.base, tc.suffix)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("historyIdentifierParts(%q,%q) = %#v, want %#v", tc.base, tc.suffix, got, tc.want)
			}
		})
	}
}

func TestQuoteQualifiedIdentifier(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   []string
		want string
	}{
		{name: "simple", in: []string{"orders_history"}, want: `"orders_history"`},
		{name: "schema qualified", in: []string{"public", "orders_history"}, want: `"public"."orders_history"`},
		{name: "needs escaping", in: []string{`Order"Detail`}, want: `"Order""Detail"`},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := quoteQualifiedIdentifier(tc.in)
			if got != tc.want {
				t.Fatalf("quoteQualifiedIdentifier(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQualifiedRegclassLiteral(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   []string
		want string
	}{
		{name: "simple", in: []string{"orders_history"}, want: `'"orders_history"'`},
		{name: "schema qualified", in: []string{"public", "orders_history"}, want: `'"public"."orders_history"'`},
		{name: "empty", in: nil, want: `''`},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := qualifiedRegclassLiteral(tc.in)
			if got != tc.want {
				t.Fatalf("qualifiedRegclassLiteral(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestBaseTableName(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple", in: "orders", want: "orders"},
		{name: "schema qualified", in: "public.orders", want: "orders"},
		{name: "quoted", in: `"Sales"."Orders"`, want: "Orders"},
		{name: "dot in quotes", in: `"Sales"."Order.Detail"`, want: "Order.Detail"},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := baseTableName(tc.in)
			if got != tc.want {
				t.Fatalf("baseTableName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
