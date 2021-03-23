// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
