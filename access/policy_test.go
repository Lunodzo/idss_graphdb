package access

import "testing"

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