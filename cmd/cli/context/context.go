package context

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/KvrocksLabs/kvrocks_controller/server/handlers"
	"github.com/KvrocksLabs/kvrocks_controller/util"
	"gopkg.in/yaml.v1"
)

const (
	DEFAULT_CONFIG_FILE  = "./kc_cli_config.yaml"
	DEFAULT_HISTORY_FILE = "./.kc_cli_history"
)

const (
	LocationRoot = iota
	LocationNamespace
	LocationCluster
)

var DEFAULT_CONTROLLERS = []string{"127.0.0.1:9379"}

type CliConf struct {
	ControllerAddrs []string `yaml:"controllers,omitempty"`
	HistoryFile     string   `yaml:"historyfile,omitempty"`
}

func LoadConfig(filePath string) (*CliConf, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	conf := &CliConf{}
	err = yaml.Unmarshal(content, conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

type Context struct {
	Leader    string
	Location  int
	Namespace string
	Cluster   string
}

var context *Context
var once sync.Once

func GetContext() *Context {
	once.Do(func() {
		context = &Context{}
	})
	return context
}

func (ctx *Context) ParserLeader(controllers []string) {
	var Err string
	for _, controllers := range controllers {
		if resp, err := util.HttpGet(handlers.GetControllerLeaderURL(controllers), nil, 5*time.Second); err == nil {
			ctx.Leader = resp.Body.(string)
			return
		} else {
			Err = err.Error()
		}
	}
	fmt.Println("get controller leader error: " + Err)
	os.Exit(1)
}

func (ctx *Context) EnterNamespace(namespace string) {
	if ctx.Location != LocationRoot {
		fmt.Println("enter namespace should under root dir")
		return
	}
	ctx.Namespace = namespace
	ctx.Location = LocationNamespace
}

func (ctx *Context) EnterCluster(cluster string) {
	if ctx.Location != LocationNamespace {
		fmt.Println("enter cluster should under namepace dir")
		return
	}
	ctx.Cluster = cluster
	ctx.Location = LocationCluster
}

func (ctx *Context) Outside() {
	switch ctx.Location {
	case LocationNamespace:
		ctx.Location = LocationRoot
		ctx.Namespace = ""
		return
	case LocationCluster:
		ctx.Location = LocationNamespace
		ctx.Cluster = ""
		return
	default:
		return
	}
	return
}
