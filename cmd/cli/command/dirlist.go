package command

import (
	"time"

	"gopkg.in/urfave/cli.v1"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	"github.com/KvrocksLabs/kvrocks-controller/server/handlers"
)
var ListCommand = cli.Command{
	Name:      "ls",
	Usage:     "list namespace or cluster",
	Action:    listAction,
	Description: `
    ls namespaces or ls clusters under special namespace
    `,
}

func listAction(c *cli.Context) {
	ctx := context.GetContext()
	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5 * time.Second)
		if HttpResponeException("list namespcae", resp, err) {
			return 
		}
		showStringList(getStringList(resp.Body), "namespace")
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5 * time.Second)
		if HttpResponeException("list cluster", resp, err) {
			return
		}
		showStringList(getStringList(resp.Body), "cluster")
	default:
		return 
	}
	return 
}