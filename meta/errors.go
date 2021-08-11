package meta

import "errors"

var (
	ErrNamespaceExisted  = errors.New("the namespace has already existed")
	ErrNamespaceNoExists = errors.New("the namespace was no exists")
	ErrClusterExisted    = errors.New("the cluster has already existed")
	ErrClusterNoExists   = errors.New("the cluster was no exists")
	ErrShardExisted      = errors.New("the shard has already existed")
	ErrShardNoExists     = errors.New("the shard was no exists")
	ErrNodeExisted       = errors.New("the node has already existed")
	ErrNodeNoExists      = errors.New("the ndoe was no exists")
)
