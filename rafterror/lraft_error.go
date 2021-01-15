package rafterror

import (
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/entity"
)

type RaftError struct {
	ErrType raft.ErrorType
	Status  entity.Status
}

func (re *RaftError) Error() string {
	return ""
}
