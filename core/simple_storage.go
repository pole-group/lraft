// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/utils"
)

/*
每个LogFile存储 4096 个 RaftLog 数据，((8 + 8 + 8) * 4096) byte 为每个 RaftLog 为文件头，组织形式为<logIndex, startPos, dataLen>

每个RaftLogFile的命名规则为（${firstLogIndex} + "_" + ${lastLogIndex}.raftLog）
*/

//simpleFileLogStorage 简单的RaftLog实现，主要是为了锻炼数据的组织代码编写以及根据自己的理解写一个简单的RaftLog存储实现
type simpleFileLogStorage struct {
	lock               sync.RWMutex
	opt                LogStorageOption
	currentRaftLogFile raftLogFile
	raftLogDir         raftLogDir
}

type raftLogFile struct {
	Name          string
	StartLogIndex int64
	EndLogIndex   int64
}

func (r raftLogFile) Compare(other raftLogFile) int {
	if r.EndLogIndex < other.StartLogIndex {
		return -1
	}
	if r.StartLogIndex > other.EndLogIndex {
		return 1
	}
	if r.StartLogIndex > other.StartLogIndex && r.EndLogIndex < other.EndLogIndex {
		panic(fmt.Errorf("imposible to produce this raftLogFile metadata info, this is ERROR"))
	}
	return 0
}

type raftLogDir struct {
	logFiles []raftLogFile
}

func (rld raftLogDir) size() int {
	return len(rld.logFiles)
}

func (rld raftLogDir) get(pos int) raftLogFile {
	return rld.logFiles[pos]
}

func newSimpleFileStorage(options ...LogStorageOptions) (LogStorage, error) {
	return nil, nil
}

func (sfl *simpleFileLogStorage) GetFirstLogIndex() int64 {
	return 0
}

func (sfl *simpleFileLogStorage) GetLastLogIndex() int64 {
	return 0
}

func (sfl *simpleFileLogStorage) GetEntry(index int64) *entity.LogEntry {
	f := sfl.findTargetRaftLogFile(index)

	 _, err := os.OpenFile(filepath.Join(sfl.opt.KvDir, f.Name), os.O_RDONLY, os.ModePerm)
	if err != nil {
		utils.RaftLog.Error("open raft log file : %s failed, error : %s", f.Name, err)
		return nil
	}

	return nil
}

func (sfl *simpleFileLogStorage) GetTerm(index int64) int64 {
	entry := sfl.GetEntry(index)
	if entry == nil {
		return 0
	}
	return entry.LogID.GetTerm()
}

func (sfl *simpleFileLogStorage) findTargetRaftLogFile(index int64) raftLogFile {
	defer sfl.lock.RUnlock()
	sfl.lock.RLock()
	pos := sort.Search(sfl.raftLogDir.size(), func(i int) bool {
		f := sfl.raftLogDir.get(i)
		return f.StartLogIndex <= index && f.EndLogIndex >= index
	})
	return sfl.raftLogDir.get(pos)
}

func (sfl *simpleFileLogStorage) AppendEntry(entry *entity.LogEntry) (bool, error) {
	return true, nil
}

func (sfl *simpleFileLogStorage) AppendEntries(entries []*entity.LogEntry) (int, error) {
	return 0, nil
}

func (sfl *simpleFileLogStorage) TruncatePrefix(firstIndexKept int64) bool {
	return false
}

func (sfl *simpleFileLogStorage) TruncateSuffix(lastIndexKept int64) bool {
	return false
}

func (sfl *simpleFileLogStorage) Rest(nextLogIndex int64) (bool, error) {
	return true, nil
}

func (sfl *simpleFileLogStorage) Shutdown() error {
	return nil
}
