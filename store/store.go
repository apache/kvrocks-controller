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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/logger"
	"github.com/apache/kvrocks-controller/store/engine"
)

type Store interface {
	IsReady(ctx context.Context) bool

	ListNamespace(ctx context.Context) ([]string, error)
	CreateNamespace(ctx context.Context, ns string) error
	ExistsNamespace(ctx context.Context, ns string) (bool, error)
	RemoveNamespace(ctx context.Context, ns string) error

	ListCluster(ctx context.Context, ns string) ([]string, error)
	GetCluster(ctx context.Context, ns, cluster string) (*Cluster, error)
	RemoveCluster(ctx context.Context, ns, cluster string) error
	CreateCluster(ctx context.Context, ns string, cluster *Cluster) error
	UpdateCluster(ctx context.Context, ns string, cluster *Cluster) error
	SetCluster(ctx context.Context, ns string, clusterInfo *Cluster) error

	CheckNewNodes(ctx context.Context, nodes []string) error
}

var _ Store = (*ClusterStore)(nil)

type ClusterStore struct {
	e engine.Engine

	locks         sync.Map
	eventNotifyCh chan EventPayload
	quitCh        chan struct{}
}

func NewClusterStore(e engine.Engine) *ClusterStore {
	return &ClusterStore{
		e:             e,
		eventNotifyCh: make(chan EventPayload, 100),
		quitCh:        make(chan struct{}),
	}
}

const (
	// peerTTL is how long a peer registration is considered live without a renewal.
	// Must be > peerRenewalInterval so a single missed tick doesn't evict a healthy peer.
	peerTTL = 15 * time.Second
	// peerRenewalInterval is how often RegisterSelf refreshes its timestamp in the store.
	peerRenewalInterval = 5 * time.Second
)

// parsePeerEntry parses a stored peer value of the form "addr|unixTimestamp".
// Returns ok=false for entries missing or having an unparseable timestamp field;
// callers should treat those as stale.
func parsePeerEntry(val string) (addr string, ts int64, ok bool) {
	idx := strings.LastIndex(val, "|")
	if idx < 0 {
		return val, 0, false
	}
	n, err := strconv.ParseInt(val[idx+1:], 10, 64)
	if err != nil {
		return val, 0, false
	}
	return val[:idx], n, true
}

// PeerInfo holds the ID and HTTP address of a peer controller node.
type PeerInfo struct {
	ID       string
	HTTPAddr string
}

// RegisterSelf writes this node's HTTP address into the store so other
// controllers can discover it. The value is stored as "addr|unixTimestamp"
// and refreshed every peerRenewalInterval so peers can detect liveness via
// timestamp staleness. The background goroutine runs until ctx is cancelled.
func (s *ClusterStore) RegisterSelf(ctx context.Context, httpAddr string) error {
	key := peerKeyPrefix + s.e.ID()
	val := fmt.Sprintf("%s|%d", httpAddr, time.Now().Unix())
	if err := s.e.Set(ctx, key, []byte(val)); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(peerRenewalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				v := fmt.Sprintf("%s|%d", httpAddr, time.Now().Unix())
				if err := s.e.Set(ctx, key, []byte(v)); err != nil {
					logger.Get().Warn("Failed to renew peer registration", zap.Error(err))
				}
			case <-ctx.Done():
				// Clean shutdown: delete our key immediately so peers stop seeing us
				// instead of waiting up to peerTTL for the timestamp to expire.
				delCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := s.e.Delete(delCtx, key); err != nil {
					logger.Get().Warn("Failed to deregister peer on shutdown", zap.Error(err))
				}
				return
			}
		}
	}()
	return nil
}

// ListActivePeers returns all registered peer nodes whose timestamps are within
// peerTTL, excluding this node. Entries with missing or stale timestamps are
// silently excluded so dead controllers don't block quorum.
func (s *ClusterStore) ListActivePeers(ctx context.Context) ([]PeerInfo, error) {
	selfID := s.e.ID()
	entries, err := s.e.List(ctx, peerKeyPrefix)
	if err != nil {
		return nil, err
	}
	var peers []PeerInfo
	for _, entry := range entries {
		if entry.Key == selfID {
			continue
		}
		addr, ts, ok := parsePeerEntry(string(entry.Value))
		if !ok || time.Since(time.Unix(ts, 0)) > peerTTL {
			continue // stale or unparseable — exclude from quorum
		}
		peers = append(peers, PeerInfo{
			ID:       entry.Key,
			HTTPAddr: addr,
		})
	}
	return peers, nil
}

func (s *ClusterStore) IsReady(ctx context.Context) bool {
	return s.e.IsReady(ctx)
}

// ListNamespace return the list of name of all namespaces
func (s *ClusterStore) ListNamespace(ctx context.Context) ([]string, error) {
	entries, err := s.e.List(ctx, nsPrefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

// ExistsNamespace return an indicator whether the specified namespace exists
func (s *ClusterStore) ExistsNamespace(ctx context.Context, ns string) (bool, error) {
	return s.e.Exists(ctx, appendPrefix(ns))
}

// CreateNamespace will create a namespace for clusters
func (s *ClusterStore) CreateNamespace(ctx context.Context, ns string) error {
	if has, _ := s.ExistsNamespace(ctx, ns); has {
		return consts.ErrAlreadyExists
	}
	if err := s.e.Set(ctx, appendPrefix(ns), []byte(ns)); err != nil {
		return err
	}
	s.EmitEvent(EventPayload{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandCreate,
	})
	return nil
}

// RemoveNamespace delete the specified namespace from store
func (s *ClusterStore) RemoveNamespace(ctx context.Context, ns string) error {
	if has, _ := s.ExistsNamespace(ctx, ns); !has {
		return consts.ErrNotFound
	}
	clusters, err := s.ListCluster(ctx, ns)
	if err != nil {
		return err
	}
	if len(clusters) != 0 {
		return fmt.Errorf("%w: please delete clusters first", consts.ErrForbidden)
	}
	if err := s.e.Delete(ctx, appendPrefix(ns)); err != nil {
		return err
	}
	s.EmitEvent(EventPayload{
		Namespace: ns,
		Type:      EventNamespace,
		Command:   CommandRemove,
	})
	return nil
}

func (s *ClusterStore) getLock(ns, cluster string) *sync.RWMutex {
	value, _ := s.locks.LoadOrStore(fmt.Sprintf("%s/%s", ns, cluster), &sync.RWMutex{})
	lock, _ := value.(*sync.RWMutex)
	return lock
}

// ListCluster return the list of name of cluster under the specified namespace
func (s *ClusterStore) ListCluster(ctx context.Context, ns string) ([]string, error) {
	entries, err := s.e.List(ctx, buildClusterPrefix(ns))
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

func (s *ClusterStore) existsCluster(ctx context.Context, ns, cluster string) (bool, error) {
	return s.e.Exists(ctx, buildClusterKey(ns, cluster))
}

func (s *ClusterStore) GetCluster(ctx context.Context, ns, cluster string) (*Cluster, error) {
	lock := s.getLock(ns, cluster)
	lock.RLock()
	defer lock.RUnlock()

	return s.getClusterWithoutLock(ctx, ns, cluster)
}

func (s *ClusterStore) getClusterWithoutLock(ctx context.Context, ns, cluster string) (*Cluster, error) {
	value, err := s.e.Get(ctx, buildClusterKey(ns, cluster))
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}
	var clusterInfo Cluster
	if err = json.Unmarshal(value, &clusterInfo); err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}
	return &clusterInfo, nil
}

// UpdateCluster update the Name to store under the specified namespace
func (s *ClusterStore) UpdateCluster(ctx context.Context, ns string, clusterInfo *Cluster) error {
	lock := s.getLock(ns, clusterInfo.Name)
	lock.Lock()
	defer lock.Unlock()

	oldCluster, err := s.getClusterWithoutLock(ctx, ns, clusterInfo.Name)
	if err != nil {
		return err
	}
	if oldCluster.Version.Load() > clusterInfo.Version.Load() {
		return fmt.Errorf("the cluster has been updated by others")
	}

	clusterInfo.Version.Add(1)
	clusterBytes, err := json.Marshal(clusterInfo)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	if err := s.e.Set(ctx, buildClusterKey(ns, clusterInfo.Name), clusterBytes); err != nil {
		return err
	}
	logger.Get().With(zap.String("cluster_info", string(clusterBytes))).Info("Updated the cluster version")

	s.EmitEvent(EventPayload{
		Namespace: ns,
		Cluster:   clusterInfo.Name,
		Type:      EventCluster,
		Command:   CommandUpdate,
	})
	return nil
}

// SetCluster set the cluster to store under the specified namespace but won't increase the version.
func (s *ClusterStore) SetCluster(ctx context.Context, ns string, clusterInfo *Cluster) error {
	lock := s.getLock(ns, clusterInfo.Name)
	lock.Lock()
	defer lock.Unlock()

	oldCluster, err := s.getClusterWithoutLock(ctx, ns, clusterInfo.Name)
	if err != nil {
		return err
	}
	if oldCluster.Version.Load() > clusterInfo.Version.Load() {
		return fmt.Errorf("the cluster has been updated by others")
	}

	value, err := json.Marshal(clusterInfo)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	return s.e.Set(ctx, buildClusterKey(ns, clusterInfo.Name), value)
}

func (s *ClusterStore) CreateCluster(ctx context.Context, ns string, clusterInfo *Cluster) error {
	lock := s.getLock(ns, clusterInfo.Name)
	lock.Lock()
	defer lock.Unlock()

	if exists, _ := s.existsCluster(ctx, ns, clusterInfo.Name); exists {
		return fmt.Errorf("cluster: %w", consts.ErrAlreadyExists)
	}
	clusterBytes, err := json.Marshal(clusterInfo)
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	if err := s.e.Set(ctx, buildClusterKey(ns, clusterInfo.Name), clusterBytes); err != nil {
		return err
	}
	s.EmitEvent(EventPayload{
		Namespace: ns,
		Cluster:   clusterInfo.Name,
		Type:      EventCluster,
		Command:   CommandCreate,
	})
	return nil
}

func (s *ClusterStore) RemoveCluster(ctx context.Context, ns, cluster string) error {
	lock := s.getLock(ns, cluster)
	lock.Lock()
	defer lock.Unlock()

	if exists, _ := s.existsCluster(ctx, ns, cluster); !exists {
		return consts.ErrNotFound
	}
	if err := s.e.Delete(ctx, buildClusterKey(ns, cluster)); err != nil {
		return err
	}

	s.EmitEvent(EventPayload{
		Namespace: ns,
		Cluster:   cluster,
		Type:      EventCluster,
		Command:   CommandRemove,
	})
	return nil
}

func (s *ClusterStore) CheckNewNodes(ctx context.Context, nodes []string) error {
	newNodes := make(map[string]bool, 0)
	for _, node := range nodes {
		newNodes[node] = true
	}

	namespaces, err := s.ListNamespace(ctx)
	if err != nil {
		return err
	}
	existingNodes := make([]string, 0)
	for _, ns := range namespaces {
		clusters, err := s.ListCluster(ctx, ns)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			c, err := s.GetCluster(ctx, ns, cluster)
			if err != nil {
				return err
			}
			for _, existingNode := range c.GetNodes() {
				if _, ok := newNodes[existingNode.Addr()]; ok {
					existingNodes = append(existingNodes, existingNode.Addr())
				}
			}
		}
	}
	if len(existingNodes) > 0 {
		return fmt.Errorf("node: %w: %v", consts.ErrAlreadyExists, existingNodes)
	}
	return nil
}

func (s *ClusterStore) Notify() <-chan EventPayload {
	return s.eventNotifyCh
}

func (s *ClusterStore) EmitEvent(event EventPayload) {
	s.eventNotifyCh <- event
}

func (s *ClusterStore) GetEngine() engine.Engine {
	return s.e
}

func (s *ClusterStore) LeaderChange() <-chan bool {
	return s.e.LeaderChange()
}

func (s *ClusterStore) IsLeader() bool {
	return s.e.Leader() == s.e.ID()
}

func (s *ClusterStore) Leader() string {
	return s.e.Leader()
}

func (s *ClusterStore) ID() string {
	return s.e.ID()
}

func (s *ClusterStore) Close() error {
	return s.e.Close()
}

func (s *ClusterStore) Stop() error {
	return nil
}
