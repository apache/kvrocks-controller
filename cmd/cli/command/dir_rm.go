package command

import (
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var RmCommand = cli.Command{
	Name:        "rm",
	Usage:       "Remove current namespace or rm current cluster",
	Action:      rm,
	Description: `remove current namespace or cluster`,
}

func rm(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationNamespace:
		resp, err := util.HttpDelete(handlers.GetNamespaceURL(ctx.Leader, ctx.Namespace), nil, 5*time.Second)
		if HttpResponeException("Remove namespace", resp, err) {
			return
		}
	case context.LocationCluster:
		resp, err := util.HttpDelete(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 5*time.Second)
		if HttpResponeException("Remove cluster", resp, err) {
			return
		}
	default:
		return
	}
	ctx.Outside()
	return
}
