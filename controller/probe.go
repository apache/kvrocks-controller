package controller

import (
	"fmt"
	"time"
	"errors"

	"go.uber.org/zap"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/logger"
	"github.com/KvrocksLabs/kvrocks-controller/storage"
	"github.com/KvrocksLabs/kvrocks-controller/failover"
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
func NewProbe(ns, cluster string, stor *storage.Storage, nfor *failover.Failover) *Probe{
	return &Probe{
		namespace: ns,
		cluster:   cluster,
		stor:      stor,
		nfor:      nfor,
		stopCh:    make(chan struct{}),
	}
}

// start goroutine to probe cluster nodes
func(p *Probe) start() {
	go p.probe()
}

// NodeInfo record node probe info
type NodeInfo struct {
	id   string
	addr string
}

// probe logic
func(p *Probe) probe() {
	probeTicker := time.NewTimer(time.Duration(ProbeInterval) * time.Second)
	defer probeTicker.Stop()
	for {
		select {
		case <-probeTicker.C:
			var (
				allNodes    = 0
				pfailNodes  = 0
				behindNodes = 0
				aheadNodes  = 0
			)
			probeInfos := make(map[int64][]*NodeInfo)
			cluster, err := p.stor.GetClusterCopy(p.namespace, p.cluster)
			if err != nil {
				logger.Get().With(
		    		zap.Error(err),
		    	).Error("get cluster form local error")
		    	break
			}
			for sidx, shard := range cluster.Shards {
				for _, node := range shard.Nodes {
					allNodes++
					info, err := util.ClusterInfoCmd(node.Address)
					if err != nil {
						pfailNodes++
				    	if err.Error() != ErrClustrerDown.Error() {
				    		p.nfor.AddFailoverNode(p.namespace, p.cluster, sidx, node, failover.AutoType)
				    		logger.Get().Warn("pfail node: " + node.Address)
				    	} else {
				    		logger.Get().With(
					    		zap.Error(err),
					    	).Error("get cluster form local error")
				    	}
					} else {
						probeInfos[info.ClusterMyEpoch] = append(probeInfos[info.ClusterMyEpoch], 
							&NodeInfo{
								id:   node.ID,
								addr: node.Address,
							})
					}
				}
			}

			// access newest cluster topo
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
					aheadNodes += len(nodes)
					logger.Get().With(
						zap.Any("node", nodes),
					).Warn("node version ahead")
					continue
				}
				behindNodes += len(nodes)
				logger.Get().With(
						zap.Any("node", nodes),
					).Warn("node version behind")
				for _, node := range nodes {
					if err := util.SyncClusterInfo2Node(node.addr, node.id, clusterStr, clusterVer); err != nil {
						logger.Get().With(
				    		zap.Error(err),
				    	).Error("sync cluster info to node error " + node.addr)
					}
				}
			}
			if aheadNodes != 0 || behindNodes != 0 || pfailNodes != 0 {
				logInfo := fmt.Sprintf("%s probe info, all: %d, pfail: %d, ahead: %d, behind: %d",
						util.NsClusterJoin(p.namespace, p.cluster), allNodes, pfailNodes, aheadNodes, behindNodes)
				logger.Get().Warn(logInfo)
			}
		case <-p.stopCh:
			return
		}
		probeTicker.Reset(time.Duration(ProbeInterval) * time.Second)
	}
}

// stop cluster probe
func(p *Probe) stop() {
	close(p.stopCh)
}