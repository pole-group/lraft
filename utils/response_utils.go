package utils

import (
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/proto"
)

func NewErrorResponse(code entity.RaftErrorCode, format string, args ...interface{}) *proto.ErrorResponse {
	errResp := &proto.ErrorResponse{}
	errResp.ErrorCode = int32(code)
	errResp.ErrorMsg = StringFormat(format, args)
	return errResp
}

