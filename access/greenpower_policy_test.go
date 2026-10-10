package access

import "testing"

func TestGreenPowerPolicyAllowsCIMKinds(t *testing.T) {
	p, err := LoadPolicy("../server/policy.greenpower.yaml")
	if err != nil {
		t.Fatal(err)
	}
	kinds := []string{"FailureEvent", "ACLineSegment", "AssetContainer", "Organisation"}
	for _, role := range []string{"member", "manager", "observer"} {
		if d := p.Evaluate(kinds, role); d != Allow {
			t.Errorf("role %s: decision %v, want allow", role, d)
		}
	}
	if d := p.Evaluate([]string{"Customer"}, "member"); d != Deny {
		t.Errorf("energy-community kind should fall back to deny, got %v", d)
	}
}
