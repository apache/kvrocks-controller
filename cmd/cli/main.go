package main

import (
	"fmt"
	"os"
	"os/user"
	"strings"


	"gopkg.in/urfave/cli.v1"
	"github.com/GeertJohan/go.linenoise"
	"github.com/KvrocksLabs/kvrocks-controller/util"
	c "github.com/KvrocksLabs/kvrocks-controller/cmd/cli/command"
	"github.com/KvrocksLabs/kvrocks-controller/cmd/cli/context"
)

var cmdsBase = []cli.Command{
	c.ListCommand,
	c.CdCommand,
	c.RmCommand,
}

var cmdsNamespace= []cli.Command{
	c.MakeNsCommand,
}

var cmdsCluster= []cli.Command{
	c.MkclCommand,
	c.ShowClusterCommand,
	c.RedisPdoCommand,
	c.PsyncCommand,
}

var cmdsShard= []cli.Command{
	c.AddShardCommand,
	c.DelShardCommand,
	c.MigrateCommand,
	c.MigrateSlotsCommand,
}

var cmdsNode= []cli.Command{
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

func handlCmd(cmd string, fields []string) bool {
	switch cmd {
	case "cd":
		if len(fields) > 1 {
			ctx.CdName = fields[1]
		} else {
			ctx.CdName = ""
		}
	case "mkns":
		if len(fields) == 2 {
			ctx.MknsName = fields[1]
		} else if len(fields) > 2 {
			fmt.Println("mkns need set only one param ${namespcae}")
			ctx.MknsName = ""
			return false
		} else if len(fields) < 2 {
			return false
		}
	case "do":
		ctx.ReidsArgs = fields[1:]
	case "pdo":
		ctx.ReidsArgs = fields[1:]
	}
	return true
}

func main() {
	//load config
	user, err := user.Current()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conf, err := context.LoadConfig(user.HomeDir + context.DEFAULT_CONFIG_FILE)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	
	// show help
	if len(os.Args) > 1 {
		help := `Usage:
		cli is interactive kvrocks-controller devops tool
		./cli enter interactive mode, help subcommand show usage
	 	~/.kc_cli_config file config kvrocks-controller addrs
		`
		fmt.Println(help)
		os.Exit(0)
	}
	ctx = context.GetContext()
	ctx.ParserLeader(conf.ControllerAddrs)
	if conf.HistoryFile == "" {
		conf.HistoryFile = user.HomeDir + context.DEFAULT_HISTORY_FILE
	}
	_, err = os.Stat(conf.HistoryFile)
	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(conf.HistoryFile)
		if err != nil {
			fmt.Println(conf.HistoryFile + "create failed")
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
		if !handlCmd(cmd.Name, fields) {
			continue
		}
		app := cli.NewApp()
		app.Name = cmd.Name
		app.Commands = []cli.Command{cmd}
		app.Run(append(os.Args[:1], fields...))
	}
}