package core

import (
	"github.com/pole-group/lraft/proto"
)

type DownloadingSnapshot struct {
	Request     *proto.InstallSnapshotRequest
	RequestDone *RpcRequestClosure
}

func NewDownloadingSnapshot(req *proto.InstallSnapshotRequest, done *RpcRequestClosure) *DownloadingSnapshot {
	return &DownloadingSnapshot{
		Request:     req,
		RequestDone: done,
	}
}

type SnapshotExecutorImpl struct {

}
