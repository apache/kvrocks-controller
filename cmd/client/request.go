package main

import (
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

type ClusterOptions struct {
	Nodes    []string
	Replica  int
	Password string
}

type Request struct {
	endpoint string
	restyCli *resty.Client
}

func NewRequest(endpoint string) *Request {
	restyCli := resty.New()
	restyCli.SetBaseURL(fmt.Sprintf("%s/api/v1", endpoint))
	return &Request{
		restyCli: restyCli,
	}
}

func (req *Request) ListNamespace() ([]string, error) {
	rsp, err := req.restyCli.R().Get("/namespaces")
	if err != nil {
		return nil, err
	}
	var result struct {
		Namespaces []string `json:"namespaces"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return nil, err
	}
	return result.Namespaces, nil
}

func (req *Request) ListCluster(ns string) ([]string, error) {
	return nil, nil
}

func (req *Request) CreateNamespace(ns string) error {
	return nil
}

func (req *Request) CreateCluster(ns string, options *ClusterOptions) error {
	return nil
}

func (req *Request) IsNamespaceExists(ns string) (bool, error) {
	return false, nil
}

func (req *Request) IsClusterExists(ns, cluster string) (bool, error) {
	return false, nil
}

func (req *Request) DeleteNamespace(ns string) error {
	return nil
}

func (req *Request) DeleteCluster(ns, cluster string) error {
	return nil
}
