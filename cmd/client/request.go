/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/RocksLabs/kvrocks_controller/metadata"

	"github.com/go-resty/resty/v2"
)

type Error struct {
	Message string `json:"message"`
}

type ClusterOptions struct {
	Name     string   `json:"name"`
	Nodes    []string `json:"nodes"`
	Replica  int      `json:"replica"`
	Password string   `json:"password"`
}

type Request struct {
	endpoint string
	restyCli *resty.Client
}

func NewRequest(endpoint string) *Request {
	endpoint = strings.TrimSuffix(endpoint, "/")
	restyCli := resty.New()
	restyCli.SetBaseURL(fmt.Sprintf("%s/api/v1", endpoint))
	return &Request{
		endpoint: endpoint,
		restyCli: restyCli,
	}
}

func (req *Request) ListNamespace() ([]string, error) {
	rsp, err := req.restyCli.R().Get("/namespaces")
	if err != nil {
		return nil, err
	}

	var result struct {
		Error *Error `json:"error"`
		Data  struct {
			Namespaces []string `json:"namespaces"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf(result.Error.Message)
	}
	return result.Data.Namespaces, nil
}

func (req *Request) ListCluster(ns string) ([]string, error) {
	path := fmt.Sprintf("/namespaces/%s/clusters", ns)
	rsp, err := req.restyCli.R().Get(path)
	if err != nil {
		return nil, err
	}
	var result struct {
		Error *Error `json:"error"`
		Data  struct {
			Clusters []string `json:"clusters"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf(result.Error.Message)
	}
	return result.Data.Clusters, nil
}

func (req *Request) CreateNamespace(ns string) error {
	rsp, err := req.restyCli.R().SetBody(
		map[string]interface{}{
			"namespace": ns,
		}).Post("/namespaces")
	if err != nil {
		return err
	}
	var result struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return err
	}
	if result.Error != nil {
		return fmt.Errorf(result.Error.Message)
	}
	if rsp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("create namespace %s failed: %s", ns, rsp.Status())
	}
	return nil
}

func (req *Request) CreateCluster(ns string, options *ClusterOptions) error {
	path := fmt.Sprintf("/namespaces/%s/clusters", ns)
	rsp, err := req.restyCli.R().SetBody(options).Post(path)
	if err != nil {
		return err
	}
	var result struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return err
	}
	if result.Error != nil {
		return fmt.Errorf(result.Error.Message)
	}
	if rsp.StatusCode() != http.StatusCreated {
		return fmt.Errorf("create clsuter failed: %s", rsp.Status())
	}
	return nil
}

func (req *Request) IsNamespaceExists(ns string) (bool, error) {
	path := fmt.Sprintf("/namespaces/%s", ns)
	rsp, err := req.restyCli.R().Get(path)
	if err != nil {
		return false, err
	}
	var result struct {
		Error *Error `json:"error"`
		Data  struct {
			Exists bool `json:"exists"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return false, err
	}
	if result.Error != nil {
		return false, fmt.Errorf(result.Error.Message)
	}
	return result.Data.Exists, nil
}

func (req *Request) IsClusterExists(ns, cluster string) (bool, error) {
	clusterInfo, err := req.GetCluster(ns, cluster)
	if err != nil {
		return false, err
	}
	return clusterInfo != nil, nil
}

func (req *Request) GetCluster(ns, cluster string) (*metadata.Cluster, error) {
	path := fmt.Sprintf("/namespaces/%s/clusters/%s", ns, cluster)
	rsp, err := req.restyCli.R().Get(path)
	if err != nil {
		return nil, err
	}
	var result struct {
		Error *Error `json:"error"`
		Data  struct {
			Cluster *metadata.Cluster `json:"cluster"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf(result.Error.Message)
	}
	if rsp.StatusCode() == http.StatusNotFound {
		return nil, errors.New("cluster not found")
	}
	if rsp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("get clsuter %s failed: %s", cluster, rsp.Status())
	}
	return result.Data.Cluster, nil
}

func (req *Request) DeleteNamespace(ns string) error {
	path := fmt.Sprintf("/namespaces/%s", ns)
	rsp, err := req.restyCli.R().Delete(path)
	if err != nil {
		return err
	}
	var result struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return err
	}
	if result.Error != nil {
		return fmt.Errorf(result.Error.Message)
	}
	if rsp.IsError() {
		return fmt.Errorf("delete namespace %s failed: %s", ns, rsp.Status())
	}
	return nil
}

func (req *Request) DeleteCluster(ns, cluster string) error {
	path := fmt.Sprintf("/namespaces/%s/clusters/%s", ns, cluster)
	rsp, err := req.restyCli.R().Delete(path)
	if err != nil {
		return err
	}
	var result struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(rsp.Body(), &result); err != nil {
		return err
	}
	if result.Error != nil {
		return fmt.Errorf(result.Error.Message)
	}
	if rsp.IsError() {
		return fmt.Errorf("delete cluster %s failed: %s", cluster, rsp.Status())
	}
	return nil
}
