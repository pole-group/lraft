package core

import (
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/utils"
)

func NewErrorResponse(code entity.RaftErrorCode, format string, args ...interface{}) *ErrorResponse {
	errResp := &ErrorResponse{}
	errResp.ErrorCode = int32(code)
	errResp.ErrorMsg = utils.StringFormat(format, args)
	return errResp
}

