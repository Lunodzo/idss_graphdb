/*
 * ! \file policy.go
 * Per-peer access-control policy evaluation for IDSS queries.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package access

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Decision int

const (
	Allow Decision = iota
	AggregateOnly
	Deny
)

func (d Decision) String() string {
	switch d {
	case Allow:
		return "allow"
	case AggregateOnly:
		return "aggregate"
	case Deny:
		return "deny"
	default:
		return "unknown"
	}
}

type Rule struct {
	Kind     string   `yaml:"kind"`
	Roles    []string `yaml:"roles"`
	Decision string   `yaml:"decision"`
}

type Policy struct {
	Default string `yaml:"default"`
	Rules   []Rule `yaml:"rules"`
}

func LoadPolicy(path string) (*Policy, error) {
	policyData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading policy file: %v", err)
	}

	policy := &Policy{}
	if err := yaml.Unmarshal(policyData, policy); err != nil {
		return nil, fmt.Errorf("parsing policy file: %v", err)
	}
	if _, err := parseDecision(policy.Default); err != nil {
		return nil, fmt.Errorf("invalid policy default: %v", err)
	}
	for index, rule := range policy.Rules {
		if rule.Kind == "" {
			return nil, fmt.Errorf("policy rule %d has no kind", index)
		}
		if len(rule.Roles) == 0 {
			return nil, fmt.Errorf("policy rule %d has no roles", index)
		}
		if _, err := parseDecision(rule.Decision); err != nil {
			return nil, fmt.Errorf("invalid decision in policy rule %d: %v", index, err)
		}
	}
	return policy, nil
}

func (p *Policy) Evaluate(kinds []string, role string) Decision {
	if len(kinds) == 0 {
		decision, _ := parseDecision(p.Default)
		return decision
	}

	decision := Allow
	for _, kind := range kinds {
		kindDecision := p.evaluateKind(kind, role)
		if kindDecision > decision {
			decision = kindDecision
		}
	}
	return decision
}

func (p *Policy) evaluateKind(kind string, role string) Decision {
	exactMatches := p.matchingDecisions(kind, role)
	if len(exactMatches) > 0 {
		return mostRestrictive(exactMatches)
	}
	wildcardMatches := p.matchingDecisions("*", role)
	if len(wildcardMatches) > 0 {
		return mostRestrictive(wildcardMatches)
	}
	decision, _ := parseDecision(p.Default)
	return decision
}

func (p *Policy) matchingDecisions(kind string, role string) []Decision {
	var decisions []Decision
	for _, rule := range p.Rules {
		if rule.Kind != kind || !containsRole(rule.Roles, role) {
			continue
		}
		decision, _ := parseDecision(rule.Decision)
		decisions = append(decisions, decision)
	}
	return decisions
}

func containsRole(roles []string, role string) bool {
	for _, candidate := range roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func mostRestrictive(decisions []Decision) Decision {
	decision := Allow
	for _, candidate := range decisions {
		if candidate > decision {
			decision = candidate
		}
	}
	return decision
}

func parseDecision(value string) (Decision, error) {
	switch strings.ToLower(value) {
	case "allow":
		return Allow, nil
	case "aggregate":
		return AggregateOnly, nil
	case "deny":
		return Deny, nil
	default:
		return Deny, fmt.Errorf("unknown decision %q", value)
	}
}