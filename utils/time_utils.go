// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"sync/atomic"
	"time"

	polerpc "github.com/pole-group/pole-rpc"
)

var currentTimeMs int64
var currentTimeNs int64

func init() {
	polerpc.DoTickerSchedule(func() {
		atomic.StoreInt64(&currentTimeMs, time.Now().Unix()*1000)
		atomic.StoreInt64(&currentTimeNs, time.Now().UnixNano())
	}, time.Duration(100)*time.Millisecond)
}

func GetCurrentTimeMs() int64 {
	return atomic.LoadInt64(&currentTimeMs)
}

func GetCurrentTimeNs() int64 {
	return atomic.LoadInt64(&currentTimeNs)
}
