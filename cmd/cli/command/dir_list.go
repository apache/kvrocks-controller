package command

import (
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var ListCommand = cli.Command{
	Name:        "ls",
	Usage:       "List namespace or cluster",
	Action:      list,
	Description: "ls {namespaces} or {cluster}",
}

func list(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5*time.Second)
		if responseError("List namespace", resp, err) {
			return
		}
		showStringList(getStringList(resp.Body), "List namespace")
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5*time.Second)
		if responseError("List cluster", resp, err) {
			return
		}
		showStringList(getStringList(resp.Body), "List Cluster")
	default:
		return
	}
}
