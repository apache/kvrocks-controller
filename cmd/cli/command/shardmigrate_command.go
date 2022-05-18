package command

import (
	"fmt"

	"gopkg.in/urfave/cli.v1"
)

var MigrateCommand = cli.Command{
	Name:      "migrate",
	Usage:     "migrate slots",
	Action:    migrateAction,
	Description: `
    migrate slots from source shard to target shard under special cluster
    `,
}

func migrateAction(c *cli.Context) {
	fmt.Println("support furture")
}