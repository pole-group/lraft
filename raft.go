package lraft

import (
	"lraft/entity"
	"lraft/rafterror"
	"lraft/storage"
)

type Iterator interface {
	GetData() []byte

	GetIndex() int64

	GetTerm() int64

	Done() Closure

	SetErrorAndRollback(nTail int64, st entity.Status)
}

type StateMachine interface {
	OnApply(iterator Iterator)

	OnShutdown()

	OnSnapshotSave(writer storage.SnapshotWriter, done Closure)

	OnSnapshotLoad(reader storage.SnapshotReader) bool

	OnLeaderStart(term int64)

	OnLeaderStop(status entity.Status)

	OnError(e rafterror.RaftError)

	OnConfigurationCommitted(conf *entity.Configuration)

	OnStopFollowing(ctx entity.LeaderChangeContext)

	OnStartFollowing(ctx entity.LeaderChangeContext)
}
