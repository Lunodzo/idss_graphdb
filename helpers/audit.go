/*
 * ! \file audit.go
 * Append-only audit log of access decisions (IDSS_AUDIT_LOG, one JSON object
 * per line). Each entry records who asked for what, for which purpose, the
 * decision, and the identity (version and hash) of the policy that made it,
 * so a member or an auditor can later reconstruct every disclosure.
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package helpers

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// AuditEntry is one access decision.
type AuditEntry struct {
	Time          string   `json:"time"`
	Peer          string   `json:"peer"`
	Action        string   `json:"action"` // query | settlement
	UQID          string   `json:"uqid,omitempty"`
	Requester     string   `json:"requester"`
	Role          string   `json:"role"`
	Purpose       string   `json:"purpose,omitempty"`
	Kinds         []string `json:"kinds,omitempty"`
	Decision      string   `json:"decision"`
	PolicyVersion string   `json:"policy_version,omitempty"`
	PolicyHash    string   `json:"policy_hash"`
}

var (
	auditMu   sync.Mutex
	auditFile *os.File
	auditOnce sync.Once
)

// Audit appends e to the audit log; it does nothing if IDSS_AUDIT_LOG is unset.
func Audit(e AuditEntry) {
	auditOnce.Do(func() {
		if path := os.Getenv("IDSS_AUDIT_LOG"); path != "" {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				logger.Errorf("Cannot open audit log %s: %v", path, err)
				return
			}
			auditFile = f
		}
	})
	if auditFile == nil {
		return
	}
	if e.Time == "" {
		e.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	line, err := json.Marshal(e)
	if err != nil {
		return
	}
	auditMu.Lock()
	defer auditMu.Unlock()
	_, _ = auditFile.Write(append(line, '\n'))
}
