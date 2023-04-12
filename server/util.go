package server

import (
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"github.com/gin-gonic/gin"
)

type Error struct {
	Message string `json:"message"`
}

type Response struct {
	Error *Error      `json:"error,omitempty"`
	Data  interface{} `json:"data"`
}

func responseOK(c *gin.Context, data interface{}) {
	responseData(c, http.StatusOK, data)
}

func responseCreated(c *gin.Context, data interface{}) {
	responseData(c, http.StatusCreated, data)
}

func responseBadRequest(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, Response{
		Error: &Error{Message: err.Error()},
	})
}

func responseData(c *gin.Context, code int, data interface{}) {
	c.JSON(code, Response{
		Data: data,
	})
}

func responseError(c *gin.Context, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, metadata.ErrEntryNoExists) {
		code = http.StatusNotFound
	} else if errors.Is(err, metadata.ErrEntryExisted) {
		code = http.StatusConflict
	}
	c.JSON(code, Response{
		Error: &Error{Message: err.Error()},
	})
}

func IsIPPort(s string) bool {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return false
	}
	return IsIP(parts[0]) && IsPort(parts[1])
}

func IsIP(ip string) bool {
	return net.ParseIP(ip) != nil
}

func IsPort(port string) bool {
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return p > 0 && p < 65536
}
