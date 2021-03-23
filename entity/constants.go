// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package entity

// Raft Error Code
type RaftErrorCode int

const (
	Unknown             = RaftErrorCode(-1)
	Success             = RaftErrorCode(0)
	ERaftTimedOut       = RaftErrorCode(10001)
	EStateMachine       = RaftErrorCode(10002)
	ECatchup            = RaftErrorCode(10003)
	ELeaderMoved        = RaftErrorCode(10004)
	EStepEer            = RaftErrorCode(10005)
	ENodeShutdown       = RaftErrorCode(10006)
	EHigherTermRequest  = RaftErrorCode(10007)
	EHigherTermResponse = RaftErrorCode(10008)
	EBadNode            = RaftErrorCode(10009)
	EVoteForCandidate   = RaftErrorCode(10010)
	ENewLeader          = RaftErrorCode(10011)
	ELeaderConflict     = RaftErrorCode(10012)
	ETransferLeaderShip = RaftErrorCode(10013)
	ELogDeleted         = RaftErrorCode(10014)
	ENoMoreUserLog      = RaftErrorCode(10015)
	ERequest            = RaftErrorCode(1000)
	EStop               = RaftErrorCode(1001)
	Eagain              = RaftErrorCode(1002)
	Eintr               = RaftErrorCode(1003)
	EInternal           = RaftErrorCode(1004)
	ECanceled           = RaftErrorCode(1005)
	EHostDown           = RaftErrorCode(1006)
	EShutdown           = RaftErrorCode(1007)
	EPerm               = RaftErrorCode(1008)
	Ebusy               = RaftErrorCode(1009)
	ETimeout            = RaftErrorCode(1010)
	EStale              = RaftErrorCode(1011)
	Enoent              = RaftErrorCode(1012)
	EExists             = RaftErrorCode(1013)
	EIO                 = RaftErrorCode(1014)
	Einval              = RaftErrorCode(1015)
	Eacces              = RaftErrorCode(1016)
)
