package handlers

import (
	"net/http"

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

func responseError(c *gin.Context, msg string) {
	c.JSON(http.StatusInternalServerError, Response{
		Error: &Error{Message: msg},
	})
}
