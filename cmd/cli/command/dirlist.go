package command

import (
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var ListCommand = cli.Command{
	Name:   "ls",
	Usage:  "list namespace or cluster",
	Action: listAction,
	Description: `
    ls namespaces or ls clusters under special namespace
    `,
}

func listAction(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5*time.Second)
		if HttpResponeException("list namespcae", resp, err) {
			return
		}
		showStringList(getStringList(resp.Body), "namespace")
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5*time.Second)
		if HttpResponeException("list cluster", resp, err) {
			return
		}
		showStringList(getStringList(resp.Body), "cluster")
	default:
		return
	}
	return
}
