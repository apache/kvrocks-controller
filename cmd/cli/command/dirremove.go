package command

import (
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)

var RmCommand = cli.Command{
	Name:   "rm",
	Usage:  "rm current namespaces or rm current cluster",
	Action: rmAction,
	Description: `
    rm current namespaces or current cluster
    if rm namespaces, the namespaces must not contain clusters
    `,
}

func rmAction(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationNamespace:
		resp, err := util.HttpDelete(handlers.GetNamespaceURL(ctx.Leader, ctx.Namespace), nil, 5 * time.Second)
		if HttpResponeException("rm namespcae", resp, err) {
			return
		}
	case context.LocationCluster:
		resp, err := util.HttpDelete(handlers.GetClusterURL(ctx.Leader, ctx.Namespace, ctx.Cluster), nil, 5 * time.Second)
		if HttpResponeException("rm cluster", resp, err) {
			return
		}
	default:
		return 
	}
	ctx.Outside()
	return 
}