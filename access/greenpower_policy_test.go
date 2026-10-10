package access

import "testing"

func TestGreenPowerPolicyAllowsCIMKinds(t *testing.T) {
	p, err := LoadPolicy("../server/policy.greenpower.yaml")
	if err != nil {
		t.Fatal(err)
	}
	kinds := []string{"FailureEvent", "ACLineSegment", "AssetContainer", "Organisation"}
	for _, role := range []string{"member", "manager", "observer", PublicRole, "coordinator"} {
		if d := p.Evaluate(kinds, role); d != Allow {
			t.Errorf("role %s: decision %v, want allow", role, d)
		}
	}
	if d := p.Evaluate([]string{"Customer"}, "member"); d != Deny {
		t.Errorf("energy-community kind should fall back to deny, got %v", d)
	}
}

func TestRequestersHiddenAndWildcardRole(t *testing.T) {
	p := &Policy{
		Default:    "deny",
		Rules:      []Rule{{Kind: "FailureEvent", Roles: []string{"coordinator"}, Decision: "allow"}, {Kind: "MeasurementValue", Roles: []string{"*"}, Decision: "allow"}},
		Requesters: map[string]string{"QmCoord": "coordinator"},
		Hidden:     []string{"maxLoadingPct"},
	}
	if r := p.RoleFor("QmCoord"); r != "coordinator" {
		t.Fatalf("RoleFor(listed) = %q", r)
	}
	if r := p.RoleFor("QmSomeoneElse"); r != PublicRole {
		t.Fatalf("RoleFor(unlisted) = %q", r)
	}
	if d := p.Evaluate([]string{"FailureEvent"}, p.RoleFor("QmCoord")); d != Allow {
		t.Fatalf("coordinator decision = %v", d)
	}
	if d := p.Evaluate([]string{"FailureEvent"}, p.RoleFor("QmX")); d != Deny {
		t.Fatalf("public decision on DSO data = %v", d)
	}
	if d := p.Evaluate([]string{"MeasurementValue"}, PublicRole); d != Allow {
		t.Fatalf("wildcard role should allow public: %v", d)
	}
	if !p.IsHidden("Maxloadingpct") || p.IsHidden("lengthKm") {
		t.Fatalf("IsHidden mismatch")
	}
}
