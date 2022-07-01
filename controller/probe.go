package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/logger"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"go.uber.org/zap"
)

var (
	// ErrClustrerDown return from kvnodes
	ErrClustrerDown = errors.New("CLUSTERDOWN The cluster is not initialized")
)

var (
	// probe interval
	ProbeInterval = failover.FailoverInterval / 2
)

// Probe manager cluster schedule
type Probe struct {
	namespace string
	cluster   string
	stor      *storage.Storage
	nfor      *failover.Failover

	stopCh chan struct{}
}

// NewProbe return Probe stands cluster probe
func NewProbe(ns, cluster string, stor *storage.Storage, nfor *failover.Failover) *Probe {
	return &Probe{
		namespace: ns,
		cluster:   cluster,
		stor:      stor,
		nfor:      nfor,
		stopCh:    make(chan struct{}),
	}
}

// start goroutine to probe cluster nodes
func (p *Probe) start() {
	go p.probe()
}

// NodeInfo record node probe info
type NodeInfo struct {
	Id   string
	Addr string
}

func (p *Probe) probe() {
	probeTicker := time.NewTimer(time.Duration(ProbeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			allNodes := 0
			probeFailureNodes := 0
			olderVersionNodes := 0
			newerVersionNodes := 0
			probeInfos := make(map[int64][]*NodeInfo)
			cluster, err := p.stor.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("get cluster form local error")
				break
			}
			for index, shard := range cluster.Shards {
				for _, node := range shard.Nodes {
					allNodes++
					info, err := util.ClusterInfoCmd(node.Address)
					if err != nil {
						probeFailureNodes++
						if err.Error() != ErrClustrerDown.Error() {
							_ = p.nfor.AddFailoverNode(p.namespace, p.cluster, index, node, failover.AutoType)
							logger.Get().Warn("pfail node: " + node.Address)
						} else {
							logger.Get().With(
								zap.Error(err),
							).Error("cluster info error")
							continue
						}
					} else {
						probeInfos[info.ClusterMyEpoch] = append(probeInfos[info.ClusterMyEpoch],
							&NodeInfo{
								Id:   node.ID,
								Addr: node.Address,
							})
					}
				}
			}

			clusterNewest, err := p.stor.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("get cluster form local error")
			} else {
				cluster = clusterNewest
			}
			clusterVer := cluster.Version
			clusterStr, err := cluster.ToSlotString()
			if err != nil {
				logger.Get().With(
					zap.Error(err),
				).Error("cluster info to string error")
				break
			}
			for ver, nodes := range probeInfos {
				if ver == clusterVer {
					continue
				}
				if ver > clusterVer {
					newerVersionNodes += len(nodes)
					logger.Get().With(
						zap.Int64("cluster_version", clusterVer),
						zap.Int64("node_version", ver),
						zap.Int("nodes", len(nodes)),
					).Warn("node version ahead")
					continue
				}
				olderVersionNodes += len(nodes)
				for _, node := range nodes {
					logger.Get().With(
						zap.Int64("cluster_version", clusterVer),
						zap.Int64("node_version", ver),
						zap.Any("node", node),
					).Warn("node version behind")
					if err := util.SyncClusterInfo2Node(node.Addr, node.Id, clusterStr, clusterVer); err != nil {
						logger.Get().With(
							zap.Error(err),
						).Error("sync cluster info to node error " + node.Addr)
					}
				}
			}
			if newerVersionNodes != 0 || olderVersionNodes != 0 || probeFailureNodes != 0 {
				logInfo := fmt.Sprintf("%s probe info, all: %d, pfail: %d, ahead: %d, behind: %d",
					util.NsClusterJoin(p.namespace, p.cluster), allNodes, probeFailureNodes, newerVersionNodes, olderVersionNodes)
				logger.Get().Warn(logInfo)
			}
		case <-p.stopCh:
			return
		}
		probeTicker.Reset(time.Duration(ProbeInterval) * time.Second)
	}
}

// stop cluster probe
func (p *Probe) stop() {
	close(p.stopCh)
}
