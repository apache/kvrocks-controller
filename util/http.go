package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"
)

type Error struct {
	Message string
}
type Response struct {
	Error *Error      `json:"error,omitempty"`
	Data  interface{} `json:"data"`
}

func ResponseOK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Data: data,
	})
}

func ResponseCreated(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Response{
		Data: data,
	})
}

func ResponseErrorWithCode(c *gin.Context, code int, msg string) {
	c.JSON(code, Response{
		Error: &Error{Message: msg},
	})
}

func ResponseError(c *gin.Context, msg string) {
	c.JSON(http.StatusInternalServerError, Response{
		Error: &Error{Message: msg},
	})
}

func do(method, url string, in interface{}, timeout time.Duration) (*Response, error) {
	reqJson, _ := json.Marshal(in)
	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := http.DefaultClient
	client.Timeout = timeout
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		var rsp Response
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		err = d.Decode(&rsp)
		return &rsp, err
	}
	return nil, errors.New(http.StatusText(resp.StatusCode))
}

func HttpPost(url string, in interface{}, timeout time.Duration) (*Response, error) {
	return do("POST", url, in, timeout)
}

func HttpPut(url string, in interface{}, timeout time.Duration) (*Response, error) {
	return do("PUT", url, in, timeout)
}

func HttpGet(url string, in interface{}, timeout time.Duration) (*Response, error) {
	return do("GET", url, in, timeout)
}

func HttpDelete(url string, in interface{}, timeout time.Duration) (*Response, error) {
	return do("DELETE", url, in, timeout)
}
