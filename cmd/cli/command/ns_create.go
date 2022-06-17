package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var CreateNamespaceCommand = cli.Command{
	Name:        "create_namespace",
	Usage:       "Create namespcae",
	ArgsUsage:   "create_namespace ${namespace}",
	Action:      createNamespace,
	Description: "Create a new namespace",
}

func createNamespace(c *cli.Context) {
	if len(c.Args()) <= 1 {
		fmt.Println("Missing namespace param")
		return
	}
	name := c.Args()[0]
	if strings.Contains(name, "/") {
		fmt.Println("The namespace name can't contain '/'")
		return
	}
	ctx := context.GetContext()
	if ctx.Location != context.LocationRoot {
		fmt.Println("Create the new namespace should under root dir '/'")
		return
	}

	resp, err := util.HttpPost(handlers.GetNamespaceRootURL(ctx.Leader), handlers.CreateNamespaceParam{Namespace: name}, 5*time.Second)
	if HttpResponeException("Create namespace", resp, err) {
		return
	}
}
