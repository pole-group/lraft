package storage

import (
	"github.com/golang/protobuf/proto"

	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
)

const (
	raftSnapshotMetaFile   = "__raft_snapshot_meta"
	raftSnapshotPrefix     = "snapshot_"
	RemoteSnapshotURISchema = "remote://"
)

type SnapshotThrottle interface {
	ThrottledByThroughput(bytes int64) int64
}

type Snapshot interface {
	GetPath() string

	ListFiles() []string

	GetFileMeta(fileName string) proto.Message
}

type SnapshotReader interface {
	Snapshot

	Status() entity.Status

	Load() *core.SnapshotMeta

	GenerateURIForCopy() string
}

type SnapshotWriter interface {
	SaveMeta(meta core.SnapshotMeta) bool

	AddFile(fileName string, meta proto.Message)

	RemoveFile(fileName string)

	Close(keepDataOnError bool)
}

type SnapshotCopier interface {
	Cancel()

	Join()

	Start()

	GetReader() SnapshotReader
}

type LastLogIndexListener interface {
	OnLastLogIndexChanged(lastLogIndex int64)
}

type NewLogCallback interface {
	OnNewLog(arg interface{}, errCode int32)
}

type LogStorage interface {
	GetFirstLogIndex() int64

	GetLastLogIndex() int64

	GetEntry(index int64) *entity.LogEntry

	GetTerm(index int64) int64

	AppendEntry(entry *entity.LogEntry) bool

	AppendEntries(entries []*entity.LogEntry) int

	TruncatePrefix(firstIndexKept int64) bool

	TruncateSuffix(lastIndexKept int64) bool

	Rest(nextLogIndex int64) bool
}

type LogManager interface {
	AddLastLogIndexListener(listener LastLogIndexListener)

	RemoveLogIndexListener(listener LastLogIndexListener)

	Join()

	AppendEntries(entries []*entity.LogEntry, done core.StableClosure)

	SetSnapshot(meta core.SnapshotMeta)

	ClearBufferedLogs()

	GetEntry(index int64) *entity.LogEntry

	GetTerm(index int64) int64

	GetFirstLogIndex() int64

	GetLastLogIndex() int64

	GetLastLogID(isFlush bool) *entity.LogId

	GetConfiguration(index int64) *entity.ConfigurationEntry

	CheckAndSetConfiguration(current *entity.ConfigurationEntry)

	Wait(expectedLastLogIndex int64, cb NewLogCallback, arg interface{}) int64

	RemoveWaiter(id int64) bool

	SetAppliedID(appliedID *entity.LogId)

	CheckConsistency() entity.Status
}
