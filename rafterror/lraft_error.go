package rafterror

import (
	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
)

type RaftError struct {
	ErrType raft.ErrorType
	Status  entity.Status
}

func (re *RaftError) Error() string {
	return ""
}
