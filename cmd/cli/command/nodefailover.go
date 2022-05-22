package command

import (
	"fmt"

	"gopkg.in/urfave/cli.v1"
)

var FailoverCommand = cli.Command{
	Name:      "failover",
	Usage:     "failover node",
	Action:    failoverAction,
	Description: `
    failover node
    `,
}

func failoverAction(c *cli.Context) {
	fmt.Println("support furture")
}