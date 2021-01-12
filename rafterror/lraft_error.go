package rafterror

import (
	"lraft/entity"
	raft "lraft/proto"
)

type RaftError struct {
	ErrType raft.ErrorType
	Status  entity.Status
}

func (re *RaftError) Error() string {
	return ""
}
