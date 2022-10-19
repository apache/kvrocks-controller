package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

type Response struct {
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Data interface{}
}

func GetResponseData(rsp *resty.Response, data interface{}) error {
	var ret Response
	ret.Data = data
	if err := json.Unmarshal(rsp.Body(), &ret); err != nil {
		return err
	}
	if ret.Error != nil {
		return fmt.Errorf("got response err: %s with http code: %d",
			ret.Error.Message, rsp.StatusCode())
	}
	return nil
}

func (c *Client) ListNamespace(ctx context.Context) ([]string, error) {
	rsp, err := c.restyCli.R().SetContext(ctx).Get("/api/v1/namespaces")
	if err != nil {
		return nil, err
	}
	namespaces := make([]string, 0)
	if err := GetResponseData(rsp, &namespaces); err != nil {
		return nil, err
	}
	return namespaces, nil
}

func (c *Client) CreateNamespace(ctx context.Context, namespace string) error {
	rsp, err := c.restyCli.R().SetContext(ctx).SetBody(map[string]string{
		"namespace": namespace,
	}).Post("/api/v1/namespaces")
	if err != nil {
		return err
	}
	var status string
	return GetResponseData(rsp, &status)
}

func (c *Client) DeleteNamespace(ctx context.Context, namespace string) error {
	rsp, err := c.restyCli.R().SetContext(ctx).
		SetPathParam("namespace", namespace).
		Delete("/api/v1/namespaces/{namespace}")
	if err != nil {
		return err
	}

	var status string
	return GetResponseData(rsp, &status)
}
