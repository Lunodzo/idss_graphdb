package access

import (
	"testing"
	"time"
)

func TestEvaluateExactKindPrecedesWildcard(t *testing.T) {
	policy := &Policy{Default: "deny", Rules: []Rule{
		{Kind: "*", Roles: []string{"member"}, Decision: "deny"},
		{Kind: "Customer", Roles: []string{"member"}, Decision: "allow"},
	}}
	if got := policy.Evaluate([]string{"Customer"}, "member"); got != Allow {
		t.Fatalf("Evaluate() = %v, want Allow", got)
	}
}

func TestEvaluateWildcardFallback(t *testing.T) {
	policy := &Policy{Default: "deny", Rules: []Rule{
		{Kind: "*", Roles: []string{"observer"}, Decision: "aggregate"},
	}}
	if got := policy.Evaluate([]string{"UsagePoint"}, "observer"); got != AggregateOnly {
		t.Fatalf("Evaluate() = %v, want AggregateOnly", got)
	}
}

func TestEvaluateConflictingRulesUseMostRestrictiveDecision(t *testing.T) {
	policy := &Policy{Default: "allow", Rules: []Rule{
		{Kind: "Trade", Roles: []string{"manager"}, Decision: "allow"},
		{Kind: "Trade", Roles: []string{"manager"}, Decision: "deny"},
	}}
	if got := policy.Evaluate([]string{"Trade"}, "manager"); got != Deny {
		t.Fatalf("Evaluate() = %v, want Deny", got)
	}
}

func TestEvaluateUsesDefault(t *testing.T) {
	policy := &Policy{Default: "deny"}
	if got := policy.Evaluate([]string{"BatteryUnit"}, "member"); got != Deny {
		t.Fatalf("Evaluate() = %v, want Deny", got)
	}
}
func TestPurposeAndValidity(t *testing.T) {
	policy, err := parsePolicy([]byte(`
default: deny
rules:
  - kind: MeterReading
    roles: [manager]
    decision: aggregate
    purposes: [settlement]
    valid_until: 2027-03-31
  - kind: MeterReading
    roles: [observer]
    decision: aggregate
    purposes: [grid-planning]
    valid_from: 2026-01-01
`))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	later := time.Date(2027, 4, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		role, purpose string
		when          time.Time
		want          Decision
	}{
		{"manager", "settlement", at, AggregateOnly},
		{"manager", "marketing", at, Deny},     // purpose limitation
		{"manager", "", at, Deny},              // no declared purpose
		{"manager", "settlement", later, Deny}, // consent expired
		{"observer", "grid-planning", at, AggregateOnly},
		{"observer", "settlement", at, Deny},
	}
	for _, c := range cases {
		got := policy.EvaluateRequest(Request{Kinds: []string{"MeterReading"}, Role: c.role, Purpose: c.purpose, Time: c.when})
		if got != c.want {
			t.Errorf("%s/%q at %s: got %s, want %s", c.role, c.purpose, c.when.Format("2006-01-02"), got, c.want)
		}
	}
}
