/*
 * ! \file policy.go
 * Per-peer access-control policy evaluation for IDSS queries.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package access

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

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

// Rule grants a decision for a node kind to requester roles. Optional
// conditions model consent as GDPR and the Data Act require it:
//   - Purposes: the rule applies only to queries that declare one of these
//     purposes (purpose limitation); a query without a purpose never matches.
//   - ValidFrom / ValidUntil: the rule applies only within this window
//     (time-bounded consent); outside it the rule does not exist and the
//     request falls through to the wildcard or default decision.
type Rule struct {
	Kind       string   `yaml:"kind"`
	Roles      []string `yaml:"roles"`
	Decision   string   `yaml:"decision"`
	Purposes   []string `yaml:"purposes,omitempty"`
	ValidFrom  string   `yaml:"valid_from,omitempty"`
	ValidUntil string   `yaml:"valid_until,omitempty"`

	from, until time.Time
}

// Policy is a peer's access policy. It is re-read when its file changes
// (checked at most every two seconds), so a member can withdraw or narrow
// consent at run time without restarting its peer.
type Policy struct {
	Version string `yaml:"version,omitempty"`
	Default string `yaml:"default"`
	Rules   []Rule `yaml:"rules"`

	mu      sync.RWMutex
	path    string
	modTime time.Time
	checked time.Time
	hash    string
}

// Request describes what is asked of a peer.
type Request struct {
	Kinds   []string
	Role    string
	Purpose string
	Time    time.Time
}

func parsePolicy(policyData []byte) (*Policy, error) {
	policy := &Policy{}
	if err := yaml.Unmarshal(policyData, policy); err != nil {
		return nil, fmt.Errorf("parsing policy file: %v", err)
	}
	if _, err := parseDecision(policy.Default); err != nil {
		return nil, fmt.Errorf("invalid policy default: %v", err)
	}
	for index := range policy.Rules {
		rule := &policy.Rules[index]
		if rule.Kind == "" {
			return nil, fmt.Errorf("policy rule %d has no kind", index)
		}
		if len(rule.Roles) == 0 {
			return nil, fmt.Errorf("policy rule %d has no roles", index)
		}
		if _, err := parseDecision(rule.Decision); err != nil {
			return nil, fmt.Errorf("invalid decision in policy rule %d: %v", index, err)
		}
		var err error
		if rule.from, err = parseWhen(rule.ValidFrom); err != nil {
			return nil, fmt.Errorf("policy rule %d valid_from: %v", index, err)
		}
		if rule.until, err = parseWhen(rule.ValidUntil); err != nil {
			return nil, fmt.Errorf("policy rule %d valid_until: %v", index, err)
		}
	}
	sum := sha256.Sum256(policyData)
	policy.hash = hex.EncodeToString(sum[:8])
	return policy, nil
}

func parseWhen(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02", value)
}

func LoadPolicy(path string) (*Policy, error) {
	policyData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading policy file: %v", err)
	}
	policy, err := parsePolicy(policyData)
	if err != nil {
		return nil, err
	}
	policy.path = path
	if info, statErr := os.Stat(path); statErr == nil {
		policy.modTime = info.ModTime()
	}
	policy.checked = time.Now()
	return policy, nil
}

// Identity returns the policy version (if set) and a hash of the policy file,
// recorded with every audited decision.
func (p *Policy) Identity() (string, string) {
	p.maybeReload()
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Version, p.hash
}

func (p *Policy) maybeReload() {
	if p.path == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.checked) < 2*time.Second {
		return
	}
	p.checked = time.Now()
	info, err := os.Stat(p.path)
	if err != nil || info.ModTime().Equal(p.modTime) {
		return
	}
	data, err := os.ReadFile(p.path)
	if err != nil {
		return
	}
	fresh, err := parsePolicy(data)
	if err != nil {
		// Keep the previous policy; a broken edit must not open access.
		return
	}
	p.Version, p.Default, p.Rules, p.hash = fresh.Version, fresh.Default, fresh.Rules, fresh.hash
	p.modTime = info.ModTime()
}

// Evaluate keeps the original two-argument form (no purpose, now).
func (p *Policy) Evaluate(kinds []string, role string) Decision {
	return p.EvaluateRequest(Request{Kinds: kinds, Role: role, Time: time.Now()})
}

// EvaluateRequest returns the most restrictive decision over the requested
// kinds, considering only rules whose purpose and validity conditions hold.
func (p *Policy) EvaluateRequest(req Request) Decision {
	p.maybeReload()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if req.Time.IsZero() {
		req.Time = time.Now()
	}
	if len(req.Kinds) == 0 {
		decision, _ := parseDecision(p.Default)
		return decision
	}

	decision := Allow
	for _, kind := range req.Kinds {
		kindDecision := p.evaluateKind(kind, req)
		if kindDecision > decision {
			decision = kindDecision
		}
	}
	return decision
}

func (p *Policy) evaluateKind(kind string, req Request) Decision {
	exactMatches := p.matchingDecisions(kind, req)
	if len(exactMatches) > 0 {
		return mostRestrictive(exactMatches)
	}
	wildcardMatches := p.matchingDecisions("*", req)
	if len(wildcardMatches) > 0 {
		return mostRestrictive(wildcardMatches)
	}
	decision, _ := parseDecision(p.Default)
	return decision
}

func (p *Policy) matchingDecisions(kind string, req Request) []Decision {
	var decisions []Decision
	for _, rule := range p.Rules {
		if rule.Kind != kind || !containsRole(rule.Roles, req.Role) {
			continue
		}
		if len(rule.Purposes) > 0 && !containsRole(rule.Purposes, req.Purpose) {
			continue
		}
		if (!rule.from.IsZero() && req.Time.Before(rule.from)) || (!rule.until.IsZero() && req.Time.After(rule.until)) {
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
