package core

import (
	"lraft/entity"
	"lraft/utils"
)

func NewErrorResponse(code entity.RaftErrorCode, format string, args ...interface{}) *ErrorResponse {
	errResp := &ErrorResponse{}
	errResp.ErrorCode = int32(code)
	errResp.ErrorMsg = utils.StringFormat(format, args)
	return errResp
}

