package server

import (
	"errors"
	"net/http"

	"github.com/KvrocksLabs/kvrocks_controller/metadata"

	"github.com/gin-gonic/gin"
)

type Error struct {
	Message string
}

type Response struct {
	Error *Error      `json:"error,omitempty"`
	Data  interface{} `json:"data"`
}

func responseOK(c *gin.Context, data interface{}) {
	response(c, http.StatusOK, data)
}

func responseCreated(c *gin.Context, data interface{}) {
	response(c, http.StatusCreated, data)
}

func response(c *gin.Context, code int, data interface{}) {
	c.JSON(code, Response{
		Data: data,
	})
}

func responseErrorWithCode(c *gin.Context, code int, err error) {
	c.JSON(code, Response{
		Error: &Error{Message: err.Error()},
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
