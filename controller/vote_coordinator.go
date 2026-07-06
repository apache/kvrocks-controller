/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/metrics"
	"github.com/apache/kvrocks-controller/store"
)

// Voter is implemented by VoteCoordinator and by nopVoter (default single-node).
type Voter interface {
	RequestVotes(ctx context.Context, req VoteRequest) (bool, error)
}

// VoteRequest is the JSON body sent to /internal/vote on each peer.
type VoteRequest struct {
	Namespace    string `json:"namespace"`
	ClusterName  string `json:"cluster_name"`
	ShardIndex   int    `json:"shard_index"`
	FailedNodeID string `json:"failed_node_id"`
}

// VoteResponse is the JSON body returned by /internal/vote.
// The diagnostic fields (FailureCount, SoftThreshold, LastProbeAgoMs) are
// populated by the peer so the leader can log them when a peer votes NO,
// making it possible to answer "why didn't failover happen?" in production.
type VoteResponse struct {
	Vote           bool   `json:"vote"`
	Reason         string `json:"reason,omitempty"`
	FailureCount   int64  `json:"failure_count,omitempty"`
	SoftThreshold  int64  `json:"soft_threshold,omitempty"`
	LastProbeAgoMs int64  `json:"last_probe_ago_ms,omitempty"`
}

// nopVoter always approves — used as default when no coordinator is configured.
type nopVoter struct{}

func (nopVoter) RequestVotes(_ context.Context, _ VoteRequest) (bool, error) {
	return true, nil
}

// VoteCoordinator asks all active peer controllers to vote before a failover.
type VoteCoordinator struct {
	clusterStore *store.ClusterStore
	voteTimeout  time.Duration
	httpClient   *http.Client
}

// NewVoteCoordinator creates a coordinator. voteTimeout is the per-peer HTTP deadline.
func NewVoteCoordinator(s *store.ClusterStore, voteTimeout time.Duration) *VoteCoordinator {
	return &VoteCoordinator{
		clusterStore: s,
		voteTimeout:  voteTimeout,
		httpClient:   &http.Client{Timeout: voteTimeout + 500*time.Millisecond},
	}
}

// RequestVotes sends a vote request to every active peer concurrently.
// Returns true only when all active peers respond YES.
// An empty peer list (single-node deployment) returns true immediately.
// If a peer times out and its lease is still alive it is treated as NO
// (network partition protection). If its lease has expired it is excluded.
func (v *VoteCoordinator) RequestVotes(ctx context.Context, req VoteRequest) (approved bool, err error) {
	start := time.Now()
	defer func() {
		result := "approved"
		if err != nil {
			result = "error"
		} else if !approved {
			result = "blocked"
		}
		metrics.Get().VoteRoundDurationMs.With(prometheus.Labels{
			"namespace": req.Namespace, "cluster": req.ClusterName, "result": result,
		}).Observe(float64(time.Since(start).Milliseconds()))
	}()

	activePeers, err := v.clusterStore.ListActivePeers(ctx)
	if err != nil {
		return false, err
	}
	metrics.Get().ActivePeersCount.With(prometheus.Labels{}).Set(float64(len(activePeers)))
	if len(activePeers) == 0 {
		return true, nil
	}

	logger.Get().Info("Starting vote round",
		zap.String("namespace", req.Namespace),
		zap.String("cluster", req.ClusterName),
		zap.String("failed_node", req.FailedNodeID),
		zap.Int("peer_count", len(activePeers)),
	)

	type result struct {
		peerID    string
		resp      VoteResponse
		callErr   error
		elapsedMs int64
	}

	resultCh := make(chan result, len(activePeers))
	for _, peer := range activePeers {
		go func(p store.PeerInfo) {
			start := time.Now()
			voteCtx, cancel := context.WithTimeout(ctx, v.voteTimeout)
			defer cancel()
			resp, callErr := v.callVote(voteCtx, p.HTTPAddr, req)
			resultCh <- result{
				peerID:    p.ID,
				resp:      resp,
				callErr:   callErr,
				elapsedMs: time.Since(start).Milliseconds(),
			}
		}(peer)
	}

	for i := 0; i < len(activePeers); i++ {
		r := <-resultCh
		if r.callErr != nil {
			// Timed out or network error — check if the lease was alive at the
			// start of this vote round. peerTTL (15 s) >> voteTimeout (≤ 500 ms),
			// so a lease that was fresh when activePeers was fetched cannot have
			// expired by the time a call fails; re-fetching the list would return
			// identical data and waste a store round-trip per failing peer.
			if peerExists(activePeers, r.peerID) {
				logger.Get().Warn("Failover blocked: peer unreachable but lease alive",
					zap.String("peer_id", r.peerID),
					zap.Int64("elapsed_ms", r.elapsedMs),
					zap.Error(r.callErr),
				)
				metrics.Get().FailoverBlocked.With(prometheus.Labels{
					"namespace": req.Namespace, "cluster": req.ClusterName,
					"reason": "peer_unreachable",
				}).Inc()
				return false, nil
			}
			logger.Get().Info("Peer lease expired, excluding from quorum",
				zap.String("peer_id", r.peerID),
			)
			continue
		}
		if !r.resp.Vote {
			logger.Get().Warn("Failover blocked: peer voted NO",
				zap.String("peer_id", r.peerID),
				zap.Int64("elapsed_ms", r.elapsedMs),
				zap.String("reason", r.resp.Reason),
				zap.Int64("failure_count", r.resp.FailureCount),
				zap.Int64("soft_threshold", r.resp.SoftThreshold),
				zap.Int64("last_probe_ago_ms", r.resp.LastProbeAgoMs),
			)
			metrics.Get().FailoverBlocked.With(prometheus.Labels{
				"namespace": req.Namespace, "cluster": req.ClusterName,
				"reason": "peer_voted_no",
			}).Inc()
			return false, nil
		}
		logger.Get().Debug("Peer voted YES",
			zap.String("peer_id", r.peerID),
			zap.Int64("elapsed_ms", r.elapsedMs),
			zap.Int64("failure_count", r.resp.FailureCount),
		)
	}
	logger.Get().Info("Vote round approved by all peers",
		zap.String("namespace", req.Namespace),
		zap.String("cluster", req.ClusterName),
		zap.String("failed_node", req.FailedNodeID),
		zap.Int("peer_count", len(activePeers)),
	)
	return true, nil
}

func (v *VoteCoordinator) callVote(ctx context.Context, peerAddr string, req VoteRequest) (VoteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return VoteResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+peerAddr+"/internal/vote", bytes.NewReader(body))
	if err != nil {
		return VoteResponse{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := v.httpClient.Do(httpReq)
	if err != nil {
		return VoteResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return VoteResponse{}, fmt.Errorf("peer returned HTTP %d", resp.StatusCode)
	}
	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return VoteResponse{}, err
	}
	return voteResp, nil
}

func peerExists(peers []store.PeerInfo, id string) bool {
	for _, p := range peers {
		if p.ID == id {
			return true
		}
	}
	return false
}
