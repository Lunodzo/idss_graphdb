/*
 * ! \file gp_providers.go
 * Green Power use case: announce the node kinds a partition holds as DHT
 * provider records (broadcast.KindKey), so that join phase 1 finds the peers
 * of the public registers by a DHT lookup instead of asking every peer.
 */

package main

import (
	"context"
	"sort"
	"time"

	"idss/graphdb/broadcast"

	"github.com/ipfs/go-cid"
	"github.com/krotik/eliasdb/graph"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// announceDataKinds announces every node kind loaded from the partition file.
// It must run before queries arrive (query bookkeeping nodes are not data).
// Records are re-announced every 20 s while the overlay forms (a record stored
// before later peers joined can sit on peers that are no longer the key's
// closest, see startCommunityManager), then every 10 minutes.
func announceDataKinds(ctx context.Context, kadDHT *dht.IpfsDHT, gm *graph.Manager) {
	kinds := append([]string(nil), gm.NodeKinds()...)
	sort.Strings(kinds)
	keys := make([]cid.Cid, 0, len(kinds))
	for _, k := range kinds {
		key, err := broadcast.KindKey(k)
		if err != nil {
			logger.Errorf("Cannot derive DHT key for kind %s: %v", k, err)
			return
		}
		keys = append(keys, key)
	}
	announce := func() error {
		for _, key := range keys {
			pctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err := kadDHT.Provide(pctx, key, true)
			cancel()
			if err != nil {
				return err
			}
		}
		return nil
	}
	go func() {
		for {
			if err := announce(); err == nil {
				logger.Infof("Announced data kinds %v in the DHT", kinds)
				break
			} else {
				logger.Debugf("Announcing data kinds failed, retrying: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		announcedAt := time.Now()
		for {
			interval := 10 * time.Minute
			if time.Since(announcedAt) < 10*time.Minute {
				interval = 20 * time.Second
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				if err := announce(); err != nil {
					logger.Warnf("Re-announcing data kinds failed: %v", err)
				}
			}
		}
	}()
}
