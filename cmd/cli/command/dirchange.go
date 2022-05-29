package command

import (
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var CdCommand = cli.Command{
	Name:      "cd",
	Usage:     "change dir between  namespcae and cluster",
	ArgsUsage: "cd ${namespace} or cd ${clsuter}",
	Action:    cdAction,
	Description: `
    cd special namespaces or special cluster
    `,
}

func cdAction(c *cli.Context) {
	if len(c.Args()) == 0 {
		return
	}
	name := c.Args()[0]
	if len(name) == 0 {
		return
	}
	ctx := context.GetContext()
	if name == ".." {
		ctx.Outside()
		return
	}

	switch ctx.Location {
	case context.LocationRoot:
		resp, err := util.HttpGet(handlers.GetNamespaceRootURL(ctx.Leader), nil, 5*time.Second)
		if HttpResponeException("cd namespcae", resp, err) {
			return
		}
		namespaces := getStringList(resp.Body)
		for _, namespace := range namespaces {
			if name == namespace {
				ctx.EnterNamespace(name)
				return
			}
		}
		return
	case context.LocationNamespace:
		resp, err := util.HttpGet(handlers.GetClusterRootURL(ctx.Leader, ctx.Namespace), nil, 5*time.Second)
		if HttpResponeException("cd cluster", resp, err) {
			return
		}
		clusters := getStringList(resp.Body)
		for _, cluster := range clusters {
			if name == cluster {
				ctx.EnterCluster(name)
				return
			}
		}
		return
	}
	return
}
