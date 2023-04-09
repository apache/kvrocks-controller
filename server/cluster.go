package server

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/storage/persistence"

	"github.com/KvrocksLabs/kvrocks_controller/util"

	"github.com/KvrocksLabs/kvrocks_controller/controller/failover"
	"github.com/KvrocksLabs/kvrocks_controller/controller/migrate"

	"github.com/gin-gonic/gin"

	"github.com/KvrocksLabs/kvrocks_controller/consts"
	"github.com/KvrocksLabs/kvrocks_controller/metadata"
	"github.com/KvrocksLabs/kvrocks_controller/storage"
)

type CreateClusterRequest struct {
	Name     string   `json:"name"`
	Nodes    []string `json:"nodes"`
	Replicas int      `json:"replicas"`
}

func (req *CreateClusterRequest) validate() error {
	if len(req.Name) == 0 {
		return fmt.Errorf("cluster name should NOT be empty")
	}
	if len(req.Nodes) == 0 {
		return errors.New("cluster nodes should NOT be empty")
	}

	if req.Replicas == 0 {
		req.Replicas = 1
	}
	if len(req.Nodes)%req.Replicas != 0 {
		return errors.New("cluster nodes should be divisible by replica")
	}
	return nil
}

type ClusterHandler struct {
	storage *storage.Storage
}

func (handler *ClusterHandler) List(c *gin.Context) {
	namespace := c.Param("namespace")
	clusters, err := handler.storage.ListCluster(c, namespace)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, gin.H{"clusters": clusters})
}

func (handler *ClusterHandler) Get(c *gin.Context) {
	namespace := c.Param("namespace")
	clusterName := c.Param("cluster")
	cluster, err := handler.storage.GetClusterInfo(c, namespace, clusterName)
	if err != nil {
		if err != persistence.ErrKeyNotFound {
			responseError(c, err)
		} else {
			responseError(c, metadata.ErrClusterNoExists)
		}
		return
	}
	responseOK(c, gin.H{"cluster": cluster})
}

func (handler *ClusterHandler) Create(c *gin.Context) {
	namespace := c.Param("namespace")

	var req CreateClusterRequest
	if err := c.BindJSON(&req); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}
	if err := req.validate(); err != nil {
		responseErrorWithCode(c, http.StatusBadRequest, err.Error())
		return
	}

	replicas := req.Replicas
	shards := make([]metadata.Shard, len(req.Nodes)/replicas)
	slotRanges := metadata.SpiltSlotRange(len(shards))
	for i := range shards {
		shards[i].Nodes = make([]metadata.NodeInfo, 0)
		for j := 0; j < replicas; j++ {
			nodeAddr := req.Nodes[i*replicas+j]
			role := metadata.RoleMaster
			if j != 0 {
				role = metadata.RoleSlave
			}
			shards[i].Nodes = append(shards[i].Nodes, metadata.NodeInfo{
				ID:      util.GenerateNodeID(),
				Address: nodeAddr,
				Role:    role,
			})
		}
		shards[i].SlotRanges = append(shards[i].SlotRanges, slotRanges[i])
		shards[i].MigratingSlot = -1
		shards[i].ImportSlot = -1
	}
	err := handler.storage.CreateCluster(c, namespace, &metadata.Cluster{
		Name:   req.Name,
		Shards: shards,
	})
	if err != nil {
		responseError(c, err)
		return
	}
	responseCreated(c, "Created")
}

func (handler *ClusterHandler) Remove(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	err := handler.storage.RemoveCluster(c, namespace, cluster)
	if err != nil {
		responseError(c, err)
		return
	}
	response(c, http.StatusNoContent, nil)
}

func (handler *ClusterHandler) GetFailOverTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")
	failover, _ := c.MustGet(consts.ContextKeyFailover).(*failover.FailOver)
	tasks, err := failover.GetTasks(c, namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, tasks)
}

func (handler *ClusterHandler) GetMigratingTasks(c *gin.Context) {
	namespace := c.Param("namespace")
	cluster := c.Param("cluster")
	typ := c.Param("type")

	migration := c.MustGet(consts.ContextKeyMigrate).(*migrate.Migrate)
	tasks, err := migration.GetMigrateTasks(c, namespace, cluster, typ)
	if err != nil {
		responseError(c, err)
		return
	}
	responseOK(c, tasks)
}
