package handlers

import (
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
	c.JSON(http.StatusOK, Response{
		Data: data,
	})
}

func responseCreated(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Response{
		Data: data,
	})
}

func responseErrorWithCode(c *gin.Context, code int, msg string) {
	c.JSON(code, Response{
		Error: &Error{Message: msg},
	})
}

func responseError(c *gin.Context, err error) {
	metaErr, ok := err.(*metadata.Error)
	if !ok {
		c.JSON(http.StatusInternalServerError, Response{
			Error: &Error{Message: err.Error()},
		})
		return
	}

	var code int
	switch metaErr.Code {
	case metadata.CodeNoExists:
		code = http.StatusNotFound
	case metadata.CodeExisted:
		code = http.StatusConflict
	default:
		code = http.StatusInternalServerError
	}
	c.JSON(code, Response{
		Error: &Error{Message: err.Error()},
	})
}
