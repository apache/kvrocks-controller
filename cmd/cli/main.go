package main

import (
	"fmt"
	"os"

	"strings"

	linenoise "github.com/GeertJohan/go.linenoise"
	c "github.com/KvrocksLabs/kvrocks_controller/cmd/cli/command"
	"github.com/KvrocksLabs/kvrocks_controller/cmd/cli/context"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/urfave/cli.v1"
)

var cmdsBase = []cli.Command{
	c.ListCommand,
	c.CdCommand,
	c.RmCommand,
}

var cmdsNamespace = []cli.Command{
	c.MakeNsCommand,
}

var cmdsCluster = []cli.Command{
	c.MkclCommand,
	c.ShowClusterCommand,
	c.FailoverShowCommand,
	c.MigrateShowCommand,
	c.PsyncCommand,
	c.RedisPdoCommand,
}

var cmdsShard = []cli.Command{
	c.AddShardCommand,
	c.DelShardCommand,
	c.MigrateCommand,
	c.MigrateSlotsCommand,
}

var cmdsNode = []cli.Command{
	c.AddNodeCommand,
	c.DelNodeCommand,
	c.FailoverCommand,
	c.SyncCommand,
	c.RedisDoCommand,
}

var cmdmap = map[string]cli.Command{}
var ctx *context.Context

func init() {
	for _, cmd := range cmdsBase {
		cmdmap[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			cmdmap[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range cmdsNamespace {
		cmdmap[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			cmdmap[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range cmdsCluster {
		cmdmap[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			cmdmap[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range cmdsShard {
		cmdmap[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			cmdmap[cmd.ShortName] = cmd
		}
	}

	for _, cmd := range cmdsNode {
		cmdmap[cmd.Name] = cmd
		if len(cmd.ShortName) != 0 {
			cmdmap[cmd.ShortName] = cmd
		}
	}
}

func getDir() string {
	switch ctx.Location {
	case context.LocationNamespace:
		return "/" + ctx.Namespace + "/> "
	case context.LocationCluster:
		return "/" + ctx.Namespace + "/" + ctx.Cluster + "/> "
	default:
		return "/>"
	}
	return "/>"
}

func showHelp() {
	fmt.Println("List of common commands:")
	for _, cmd := range cmdsBase {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of namespcae commands:")
	for _, cmd := range cmdsNamespace {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of cluster commands:")
	for _, cmd := range cmdsCluster {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of shard commands:")
	for _, cmd := range cmdsShard {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
	fmt.Println("\nList of node commands:")
	for _, cmd := range cmdsNode {
		fmt.Printf("%-3s%-14s  -  %-50s\n", "   ", cmd.Name, cmd.Usage)
	}
}

func main() {
	var (
		conf *context.CliConf
		err  error
	)
	if len(os.Args) == 3 && string(os.Args[1]) == "--controller" {
		conf = &context.CliConf{
			ControllerAddrs: strings.Split(os.Args[2], ","),
			HistoryFile:     context.DEFAULT_HISTORY_FILE,
		}
	} else if len(os.Args) == 3 && string(os.Args[1]) == "--config" {
		conf, err = context.LoadConfig(os.Args[2])
	} else {
		conf = &context.CliConf{
			ControllerAddrs: context.DEFAULT_CONTROLLERS,
			HistoryFile:     context.DEFAULT_HISTORY_FILE,
		}
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// show help
	if len(os.Args) == 2 && (string(os.Args[1]) == "-h" || string(os.Args[1]) == "--help") {
		help := `Usage:
		cli is interactive kvrocks controller devops tool
		./cli enter interactive mode, help subcommand show usage
		--controller ${controller-1-addr, controller-2-addr, ...} set controllers list
	 	--config ${configpath} set file config(yaml) path
		`
		fmt.Println(help)
		os.Exit(0)
	}
	ctx = context.GetContext()
	ctx.ParserLeader(conf.ControllerAddrs)
	if conf.HistoryFile == "" {
		conf.HistoryFile = context.DEFAULT_HISTORY_FILE
	}
	_, err = os.Stat(conf.HistoryFile)
	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(conf.HistoryFile)
		if err != nil {
			fmt.Println(conf.HistoryFile + " create failed")
		}
	}

	err = linenoise.LoadHistory(conf.HistoryFile)
	if err != nil {
		fmt.Println(err)
	}
	defer util.RedisPoolClose()
	for {
		str, err := linenoise.Line(getDir())
		if err != nil {
			if err == linenoise.KillSignalError {
				os.Exit(1)
			}
			fmt.Printf("Unexpected error: %s\n", err)
			os.Exit(1)
		}
		fields := strings.Fields(str)

		linenoise.AddHistory(str)
		err = linenoise.SaveHistory(conf.HistoryFile)
		if err != nil {
			fmt.Println(err)
		}

		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "help":
			showHelp()
			continue
		case "quit":
			os.Exit(0)
		}

		cmd, ok := cmdmap[fields[0]]
		if !ok {
			fmt.Println("Error: unknown command.")
			continue
		}
		app := cli.NewApp()
		app.Name = cmd.Name
		app.Commands = []cli.Command{cmd}
		app.Run(append(os.Args[:1], fields...))
	}
}
