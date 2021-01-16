package entity

import (
	raft "github.com/pole-group/lraft/proto"
)

type RaftError struct {
	ErrType raft.ErrorType
	Status  Status
}

func (re *RaftError) Error() string {
	return ""
}
