// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"

	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

type pebbleLog struct{}

func (pl *pebbleLog) Infof(format string, args ...interface{}) {
	utils.RaftLog.Info(format, args...)
}

func (pl *pebbleLog) Fatalf(format string, args ...interface{}) {
	utils.RaftLog.Error(format, args...)
}

type pebbleLogStorage struct {
	opt                  *LogStorageOption
	pebbleDB             *pebble.DB
	pebbleOpt            *pebble.Options
	lock                 sync.RWMutex
	firstLogIndex        int64
	hasLoadFirstLogIndex bool
	writeOpt             *pebble.WriteOptions
}

func newPebbleStorage(options ...LogStorageOptions) (LogStorage, error) {
	storage := new(pebbleLogStorage)
	if err := storage.init(options...); err != nil {
		return nil, err
	}
	return storage, nil
}

func (pls *pebbleLogStorage) init(options ...LogStorageOptions) error {
	pls.opt = &LogStorageOption{}
	for _, option := range options {
		option(pls.opt)
	}

	pls.pebbleOpt = &pebble.Options{
		BytesPerSync:     0,
		Cache:            nil,
		Cleaner:          nil,
		Comparer:         nil,
		DebugCheck:       nil,
		DisableWAL:       false,
		ErrorIfExists:    false,
		ErrorIfNotExists: true,
		EventListener:    pebble.EventListener{},
		Experimental: struct {
			L0CompactionConcurrency   int
			CompactionDebtConcurrency int
			DeleteRangeFlushDelay     time.Duration
			MinDeletionRate           int
			ReadCompactionRate        int64
			ReadSamplingMultiplier    uint64
		}{},
		Filters:                     nil,
		FlushSplitBytes:             0,
		FS:                          nil,
		L0CompactionThreshold:       0,
		L0StopWritesThreshold:       0,
		LBaseMaxBytes:               0,
		Levels:                      nil,
		Logger:                      &pebbleLog{},
		MaxManifestFileSize:         0,
		MaxOpenFiles:                0,
		MemTableSize:                0,
		MemTableStopWritesThreshold: 0,
		Merger:                      nil,
		MaxConcurrentCompactions:    0,
		ReadOnly:                    false,
		TablePropertyCollectors:     nil,
		WALBytesPerSync:             0,
		WALDir:                      "",
		WALMinSyncInterval:          nil,
	}

	if err := pls.openDB(); err != nil {
		return err
	}

	return pls.load(pls.opt.ConfMgn)
}

func (pls *pebbleLogStorage) openDB() error {
	defer pls.lock.Unlock()
	pls.lock.Lock()

	if pls.pebbleDB != nil {
		return nil
	}

	db, err := pebble.Open(pls.opt.KvDir, pls.pebbleOpt)
	if err != nil {
		return err
	}
	pls.pebbleDB = db
	return nil
}

//destroyDB 销毁 pebble 数据库的数据
func (pls *pebbleLogStorage) destroyDB() error {
	defer pls.lock.Unlock()
	pls.lock.Lock()

	if pls.pebbleDB != nil {
		if err := pls.pebbleDB.Close(); err != nil {
			return err
		}
		if err := os.RemoveAll(pls.opt.KvDir); err != nil {
			return err
		}
		if err := os.RemoveAll(pls.opt.WALDir); err != nil {
			return err
		}
	}
	return nil
}

func (pls *pebbleLogStorage) load(confMgn *entity.ConfigurationManager) error {
	iterator := pls.pebbleDB.NewIter(&pebble.IterOptions{
		LowerBound:  nil,
		UpperBound:  nil,
		TableFilter: nil,
	})

	defer func() {
		if err := iterator.Close(); err != nil {
			utils.RaftLog.Error("occur error when exec iterator : %#v", err)
		}
	}()
	for iterator.First(); iterator.Valid(); {
		k := iterator.Key()
		var op func(v []byte) error

		if len(k) == 8 {
			op = func(v []byte) error {
				logEntry := new(raft.PBLogEntry)
				if err := proto.Unmarshal(v, logEntry); err != nil {
					utils.RaftLog.Warn("fail to unmarshal conf entry at index %s, the log data is: %s.", v, v)
					return err
				}
				if logEntry.Type == raft.EntryType_EntryTypeConfiguration {
					confEntry := new(entity.ConfigurationEntry)
					confEntry.SetID(entity.NewLogID(logEntry.Index, logEntry.Term))
					confEntry.SetConf(entity.NewConfiguration(entity.BatchParsePeerFromBytes(logEntry.Peers),
						entity.BatchParsePeerFromBytes(logEntry.Learners)))
					if logEntry.OldPeers != nil {
						confEntry.SetOldConf(entity.NewConfiguration(entity.BatchParsePeerFromBytes(logEntry.
							OldPeers), entity.BatchParsePeerFromBytes(logEntry.OldLearners)))
					}
					if confMgn != nil {
						confMgn.Add(confEntry)
					}
				}
				return nil
			}
		} else {
			op = func(v []byte) error {
				if bytes.Compare(FirstLogIdxKey, k) == 0 {
					pls.setFirstLogIndex(utils.ParseToInt64(string(v)))
					pls.truncateRangeInBackground(0, pls.firstLogIndex)
				} else {
					utils.RaftLog.Warn("unknown entry in configuration storage key=%s, value=%s.",
						hex.EncodeToString(k), hex.EncodeToString(v))
				}
				return nil
			}
		}

		if err := op(iterator.Value()); err != nil {
			return err
		}
		iterator.Next()
	}
	return nil
}

func (pls *pebbleLogStorage) setFirstLogIndex(index int64) {
	pls.hasLoadFirstLogIndex = true
	pls.firstLogIndex = index
}

//truncateRangeInBackground 后台清除一个范围内的数据记录
func (pls *pebbleLogStorage) truncateRangeInBackground(firstIndex, secondIndex int64) {
	polerpc.Go(truncateLog{firstIndex: firstIndex, secondIndex: secondIndex}, func(arg interface{}) {
		param := arg.(truncateLog)

		//TODO 这里需要验证一下， conf_%d 是否恒大于 %d，否则 range 删除数据会导致数据误删除
		err := pls.pebbleDB.DeleteRange([]byte(fmt.Sprintf(ConfPreFix+"%d", param.firstIndex)),
			[]byte(fmt.Sprintf(ConfPreFix+"%d",
				param.secondIndex)), pls.writeOpt)

		if err != nil {
			utils.RaftLog.Error("occur error when truncate conf log from index : %d to index : %d", param.firstIndex,
				param.secondIndex)
		}

		err = pls.pebbleDB.DeleteRange([]byte(fmt.Sprintf("%d", param.firstIndex)), []byte(fmt.Sprintf("%d",
			param.secondIndex)), pls.writeOpt)

		if err != nil {
			utils.RaftLog.Error("occur error when truncate user log from index : %d to index : %d", param.firstIndex,
				param.secondIndex)
		}
	})
}

//AppendEntry  追加一条 Raft 日志
func (pls *pebbleLogStorage) AppendEntry(entry *entity.LogEntry) (bool, error) {
	if pls.pebbleDB == nil {
		return false, nil
	}

	batch := pls.pebbleDB.NewBatch()

	defer func() {
		if err := batch.Close(); err != nil {
			utils.RaftLog.Error("close write batch failed, error : %s", err)
		}
		pls.lock.RUnlock()
	}()
	pls.lock.RLock()

	logIndex := entry.LogID.GetIndex()
	val := entry.Encode()

	if entry.LogType == raft.EntryType_EntryTypeConfiguration {
		if err := batch.Set([]byte(fmt.Sprintf(ConfPreFix+"%d", logIndex)), val, pls.writeOpt); err != nil {
			_ = batch.Close()
			return false, err
		}
	}

	if err := batch.Set([]byte(fmt.Sprintf("%d", logIndex)), val, pls.writeOpt); err != nil {
		_ = batch.Close()
		return false, err
	}

	if err := batch.Commit(pls.writeOpt); err != nil {
		utils.RaftLog.Error("")
		return false, err
	}
	return true, nil
}

func (pls *pebbleLogStorage) AppendEntries(entries []*entity.LogEntry) (int, error) {
	if entries == nil || len(entries) == 0 {
		return 0, nil
	}

	batch := pls.pebbleDB.NewBatch()

	defer func() {
		if err := batch.Close(); err != nil {
			utils.RaftLog.Error("close write batch failed, error : %s", err)
		}
		pls.lock.RUnlock()
	}()
	pls.lock.RLock()

	for _, entry := range entries {
		val := entry.Encode()
		logIndex := entry.LogID.GetIndex()
		if entry.LogType == raft.EntryType_EntryTypeConfiguration {
			if err := batch.Set([]byte(fmt.Sprintf(ConfPreFix+"%d", entry.LogID.GetIndex())), val, pls.writeOpt); err != nil {
				_ = batch.Close()
				return 0, err
			}
		}
		if err := batch.Set([]byte(fmt.Sprintf("%d", logIndex)), val, pls.writeOpt); err != nil {
			_ = batch.Close()
			return 0, err
		}
	}
	if err := batch.Commit(pls.writeOpt); err != nil {
		return 0, err
	}
	return len(entries), nil
}

//TruncatePrefix
func (pls *pebbleLogStorage) TruncatePrefix(firstIndexKept int64) bool {
	defer pls.lock.RUnlock()
	pls.lock.RLock()

	startIndex := pls.GetFirstLogIndex()
	saveOk, err := pls.saveFirstLogIndex(startIndex)
	if err == nil && saveOk {
		pls.setFirstLogIndex(startIndex)
	}
	pls.truncateRangeInBackground(startIndex, firstIndexKept)
	return saveOk
}

//TruncateSuffix
func (pls *pebbleLogStorage) TruncateSuffix(lastIndexKept int64) bool {
	defer pls.lock.RUnlock()
	pls.lock.RLock()
	pls.truncateRangeInBackground(lastIndexKept+1, pls.GetLastLogIndex()+1)
	return true
}

func (pls *pebbleLogStorage) Rest(nextLogIndex int64) (bool, error) {
	if nextLogIndex <= 0 {
		return false, fmt.Errorf("invalid next log index, index : %d", nextLogIndex)
	}
	defer pls.lock.Unlock()
	pls.lock.Lock()

	entry := pls.GetEntry(nextLogIndex)

	if err := pls.destroyDB(); err != nil {
		return false, err
	}

	if err := pls.load(nil); err != nil {
		return false, err
	}
	if entry == nil {
		entry = &entity.LogEntry{
			LogType:     raft.EntryType_EntryTypeNoOp,
			LogID:       entity.NewLogID(nextLogIndex, 0),
			HasChecksum: false,
		}
	}
	return pls.AppendEntry(entry)
}

//GetEntry 根据某个 index 获取对应的 raft.PBLogEntry
func (pls *pebbleLogStorage) GetEntry(index int64) *entity.LogEntry {
	defer pls.lock.RUnlock()
	pls.lock.RLock()

	// 当前的这个Index已经被压缩裁剪掉了，无法找到
	if pls.hasLoadFirstLogIndex && index < pls.firstLogIndex {
		return nil
	}

	k := []byte(fmt.Sprintf("%d", index))
	v, i, err := pls.pebbleDB.Get(k)
	if err != nil {
		utils.RaftLog.Error("get LogEntry at index : %d failed, error : %s", index, err)
		return nil
	}

	defer i.Close()
	logEntry := new(raft.PBLogEntry)

	if err := proto.Unmarshal(v, logEntry); err != nil {
		utils.RaftLog.Error("get LogEntry at index : %d success, but unmarshal failed, error : %s", index, err)
		return nil
	}

	return entity.ToLogEntry(logEntry)
}

//GetTerm
func (pls *pebbleLogStorage) GetTerm(index int64) int64 {
	entry := pls.GetEntry(index)
	if entry == nil {
		return 0
	}
	return entry.LogID.GetTerm()
}

//GetFirstLogIndex
func (pls *pebbleLogStorage) GetFirstLogIndex() int64 {
	defer pls.lock.RUnlock()
	pls.lock.RLock()

	if pls.hasLoadFirstLogIndex {
		return pls.firstLogIndex
	}

	it := pls.pebbleDB.NewIter(&pebble.IterOptions{
		LowerBound:  nil,
		UpperBound:  nil,
		TableFilter: nil,
	})

	defer func() {
		if err := it.Close(); err != nil {
			utils.RaftLog.Error("close iterator failed, error : %s", err)
		}
	}()

	it.First()
	if it.Valid() {
		firstLogIndex := utils.ParseToInt64(string(it.Value()))
		_, _ = pls.saveFirstLogIndex(firstLogIndex)
		pls.setFirstLogIndex(firstLogIndex)
		return firstLogIndex
	}
	return 1
}

//GetLastLogIndex
func (pls *pebbleLogStorage) GetLastLogIndex() int64 {
	defer pls.lock.RUnlock()
	pls.lock.RLock()

	it := pls.pebbleDB.NewIter(&pebble.IterOptions{
		LowerBound:  nil,
		UpperBound:  nil,
		TableFilter: nil,
	})

	defer func() {
		if err := it.Close(); err != nil {
			utils.RaftLog.Error("close iterator failed, error : %s", err)
		}
	}()

	it.Last()
	if it.Valid() {
		return utils.ParseToInt64(string(it.Value()))
	}
	return 0
}

//saveFirstLogIndex
func (pls *pebbleLogStorage) saveFirstLogIndex(firstLogIndex int64) (bool, error) {
	defer pls.lock.RUnlock()
	pls.lock.RLock()
	v := []byte(fmt.Sprintf("%d", firstLogIndex))
	k := FirstLogIdxKey
	if err := pls.pebbleDB.Set(k, v, pls.writeOpt); err != nil {
		return false, err
	}
	return true, nil
}

func (pls *pebbleLogStorage) Shutdown() error {
	// 尽可能将数据都写入到磁盘中
	if err := pls.pebbleDB.Flush(); err != nil {
		utils.RaftLog.Warn("flush all data to disk failed, error : %#v", err)
	}
	return pls.pebbleDB.Close()
}
