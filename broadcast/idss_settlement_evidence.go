/*
 * ! \file idss_settlement_evidence.go
 * Signed settlement totals. A member binds its reported totals to the period,
 * its community and a SHA-256 digest of the reading blocks it settled, and
 * signs the result with its libp2p identity key. The manager verifies the
 * signature against the member's authenticated peer ID before accepting the
 * totals, and stores digest and signature with the SettlementSummary.
 *
 * What this provides: integrity of the totals between member and manager,
 * non-repudiation (a member cannot later deny what it reported, and the
 * manager cannot alter a member's totals without detection), and a commitment
 * an auditor can later check against the member's reading blocks. It does
 * not prove that the reading blocks themselves are truthful; that needs
 * signed meter data or a trusted meter operator (see the paper's discussion).
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package broadcast

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"idss/graphdb/common"

	"github.com/krotik/eliasdb/graph"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// LocalSettlementEvidence returns the active-energy and trade totals for
// [from, to] together with a digest over the settled reading blocks.
func LocalSettlementEvidence(gm *graph.Manager, hostID peer.ID, from time.Time, to time.Time) (float64, float64, []byte, error) {
	period := fmt.Sprintf(` where timeStamp >= "%s" and timeStamp < "%s"`, from.Format(time.RFC3339), to.Format(time.RFC3339))
	meterRows, meterHeader, err := RunIDSSQuery("get MeterReading"+period+` and readingType = "activePower"`, hostID, gm)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("querying meter readings: %v", err)
	}
	tradeRows, tradeHeader, err := RunIDSSQuery("get Trade"+period, hostID, gm)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("querying trades: %v", err)
	}
	return sumColumn(meterRows, meterHeader, "Value"), sumColumn(tradeRows, tradeHeader, "Volume"),
		digestRows(meterHeader, meterRows), nil
}

// digestRows hashes rows in a canonical order (header, then rows sorted as
// text), so the digest does not depend on storage order.
func digestRows(header []string, rows [][]interface{}) []byte {
	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		cells := make([]string, len(row))
		for i, cell := range row {
			cells[i] = fmt.Sprint(cell)
		}
		lines = append(lines, strings.Join(cells, "\x1f"))
	}
	sort.Strings(lines)
	h := sha256.New()
	h.Write([]byte(strings.Join(header, "\x1f")))
	for _, line := range lines {
		h.Write([]byte{'\n'})
		h.Write([]byte(line))
	}
	return h.Sum(nil)
}

// settlementSigningBytes is the canonical byte string a member signs.
func settlementSigningBytes(r *common.SettlementResult) []byte {
	return []byte(fmt.Sprintf("idss-settlement-v1|%s|%s|%s|%s|%.6f|%.6f|%x",
		r.CommunityId, r.PeerId, r.From, r.To, r.MeterReadingSum, r.TradeVolumeSum, r.BlocksDigest))
}

// SignSettlement signs r with the host's identity key.
func SignSettlement(h host.Host, r *common.SettlementResult) error {
	key := h.Peerstore().PrivKey(h.ID())
	if key == nil {
		return fmt.Errorf("no private key for %s", h.ID())
	}
	sig, err := key.Sign(settlementSigningBytes(r))
	if err != nil {
		return err
	}
	r.Signature = sig
	return nil
}

// VerifySettlement checks that r was signed by remote, that it names remote
// and that it covers the requested period.
func VerifySettlement(h host.Host, remote peer.ID, r *common.SettlementResult, from, to string) error {
	if len(r.Signature) == 0 {
		return fmt.Errorf("unsigned result")
	}
	if r.PeerId != remote.String() {
		return fmt.Errorf("result names %s but came from %s", r.PeerId, remote)
	}
	if r.From != from || r.To != to {
		return fmt.Errorf("result covers %s..%s, requested %s..%s", r.From, r.To, from, to)
	}
	pub := h.Peerstore().PubKey(remote)
	if pub == nil {
		var err error
		if pub, err = remote.ExtractPublicKey(); err != nil {
			return fmt.Errorf("no public key for %s: %v", remote, err)
		}
	}
	ok, err := pub.Verify(settlementSigningBytes(r), r.Signature)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("signature does not match the reported totals")
	}
	return nil
}
