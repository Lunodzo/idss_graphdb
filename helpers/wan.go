/*
 * ! \file wan.go
 * Application-level emulation of wide-area links and peer churn for testbeds
 * whose real network is a low-latency cluster interconnect (no root access,
 * so tc-netem cannot be used). Everything here is off unless the
 * corresponding environment variable is set; DHT traffic is never affected.
 *
 * Link model, applied before every IDSS protocol message a peer writes:
 *   IDSS_LINK_DELAY_MS         fixed one-way delay (ms)
 *   IDSS_LINK_DELAY_MIN_MS,
 *   IDSS_LINK_DELAY_MAX_MS     one-way delay per link, drawn once per peer pair
 *                              uniformly from [min, max] (deterministic hash
 *                              of the two peer IDs), plus +-10 % jitter per message
 *   IDSS_LINK_LOSS             probability that a message suffers a TCP
 *                              retransmission stall; each further loss of the
 *                              retransmission doubles the stall (exponential
 *                              back-off), as TCP masks loss as delay
 *   IDSS_LINK_RTO_MS           first retransmission timeout (default 200 ms,
 *                              the Linux minimum RTO)
 *   IDSS_LINK_RATE_MBPS        sender bandwidth: adds len(frame)*8/rate
 *
 * Churn: IDSS_CHURN_FILE names a file holding "<fraction> <seed> [silent|reset]".
 * A peer whose hash(peerID, seed) falls below the fraction behaves as offline
 * for IDSS streams: "silent" (default) never answers, so senders wait for
 * their deadline as with an unreachable host; "reset" refuses at once. The
 * file is re-read at most every two seconds, so the offline set can change
 * during a run. Peers started with -manager, or with IDSS_CHURN_EXEMPT=1,
 * never go offline (they are the requesters' entry points).
 *
 * Copyright 2023-2027, University of Salento, Italy.
 * All rights reserved.
 */

package helpers

import (
	"bufio"
	"hash/fnv"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
)

type wanConfig struct {
	fixed    time.Duration
	min, max time.Duration
	loss     float64
	rto      time.Duration
	rateBps  float64
}

var (
	wanOnce sync.Once
	wan     wanConfig
	wanRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	wanMu   sync.Mutex
)

func envMillis(name string) time.Duration {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v > 0 {
		return time.Duration(v * float64(time.Millisecond))
	}
	return 0
}

func loadWAN() {
	wan.fixed = envMillis("IDSS_LINK_DELAY_MS")
	wan.min = envMillis("IDSS_LINK_DELAY_MIN_MS")
	wan.max = envMillis("IDSS_LINK_DELAY_MAX_MS")
	if wan.max < wan.min {
		wan.max = wan.min
	}
	if v, err := strconv.ParseFloat(os.Getenv("IDSS_LINK_LOSS"), 64); err == nil && v > 0 && v < 1 {
		wan.loss = v
	}
	wan.rto = envMillis("IDSS_LINK_RTO_MS")
	if wan.rto == 0 {
		wan.rto = 200 * time.Millisecond
	}
	if v, err := strconv.ParseFloat(os.Getenv("IDSS_LINK_RATE_MBPS"), 64); err == nil && v > 0 {
		wan.rateBps = v * 1e6
	}
	if wan.fixed > 0 || wan.max > 0 || wan.loss > 0 || wan.rateBps > 0 {
		logger.Infof("WAN emulation: fixed=%v range=[%v,%v] loss=%.3f rto=%v rate=%.0f bit/s",
			wan.fixed, wan.min, wan.max, wan.loss, wan.rto, wan.rateBps)
	}
}

func wanUniform() float64 {
	wanMu.Lock()
	defer wanMu.Unlock()
	return wanRand.Float64()
}

// pairFraction maps an unordered peer pair to a stable value in [0, 1).
func pairFraction(a, b string) float64 {
	if a > b {
		a, b = b, a
	}
	h := fnv.New64a()
	h.Write([]byte(a))
	h.Write([]byte{0})
	h.Write([]byte(b))
	return float64(h.Sum64()>>11) / float64(1<<53)
}

// emulatedLinkDelay returns the delay to apply before writing a frame of
// frameBytes bytes to w.
func emulatedLinkDelay(w interface{}, frameBytes int) time.Duration {
	wanOnce.Do(loadWAN)
	delay := wan.fixed
	if wan.max > 0 {
		f := wanUniform()
		if s, ok := w.(network.Stream); ok {
			f = pairFraction(s.Conn().LocalPeer().String(), s.Conn().RemotePeer().String())
		}
		base := wan.min + time.Duration(f*float64(wan.max-wan.min))
		jitter := 0.9 + 0.2*wanUniform()
		delay += time.Duration(float64(base) * jitter)
	}
	if wan.loss > 0 {
		stall := wan.rto
		for wanUniform() < wan.loss {
			delay += stall
			stall *= 2
		}
	}
	if wan.rateBps > 0 && frameBytes > 0 {
		delay += time.Duration(float64(frameBytes) * 8 / wan.rateBps * float64(time.Second))
	}
	return delay
}

// ---------------------------------------------------------------- churn

type churnState struct {
	mu       sync.Mutex
	checked  time.Time
	modTime  time.Time
	fraction float64
	seed     string
	mode     string
}

var churn churnState

func (c *churnState) refresh() {
	path := os.Getenv("IDSS_CHURN_FILE")
	if path == "" {
		return
	}
	if time.Since(c.checked) < 2*time.Second {
		return
	}
	c.checked = time.Now()
	info, err := os.Stat(path)
	if err != nil {
		c.fraction = 0
		return
	}
	if info.ModTime().Equal(c.modTime) {
		return
	}
	c.modTime = info.ModTime()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		c.fraction = 0
		return
	}
	fields := strings.Fields(sc.Text())
	c.fraction, c.seed, c.mode = 0, "", "silent"
	if len(fields) > 0 {
		if v, err := strconv.ParseFloat(fields[0], 64); err == nil && v >= 0 && v <= 1 {
			c.fraction = v
		}
	}
	if len(fields) > 1 {
		c.seed = fields[1]
	}
	if len(fields) > 2 && fields[2] == "reset" {
		c.mode = "reset"
	}
	logger.Infof("Churn: fraction=%.3f seed=%s mode=%s", c.fraction, c.seed, c.mode)
}

// ChurnOffline reports whether peerID is currently emulated as offline, and
// in which mode ("silent" or "reset"). exempt peers are never offline.
func ChurnOffline(peerID string, exempt bool) (bool, string) {
	if exempt || os.Getenv("IDSS_CHURN_EXEMPT") == "1" {
		return false, ""
	}
	churn.mu.Lock()
	defer churn.mu.Unlock()
	churn.refresh()
	if churn.fraction <= 0 {
		return false, ""
	}
	return pairFraction(peerID, "churn:"+churn.seed) < churn.fraction, churn.mode
}

// SelectedByFraction reports whether peerID falls in a deterministic subset
// holding the given fraction of all peers (salted by label). Used to pick the
// peers of a probe (e.g. IDSS_TAMPER_SETTLEMENT=0.1).
func SelectedByFraction(peerID, label string, fraction float64) bool {
	return fraction > 0 && pairFraction(peerID, label) < fraction
}
