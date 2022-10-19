package client

import "github.com/go-resty/resty/v2"

type Client struct {
	restyCli *resty.Client
}

func New(host string) *Client {
	if host == "" {
		host = "http://127.0.0.1:9379"
	}
	restyCli := resty.New()
	restyCli.SetBaseURL(host)
	return &Client{
		restyCli: restyCli,
	}
}
