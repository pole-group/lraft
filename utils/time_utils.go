package utils

import (
	"context"
	"sync/atomic"
	"time"
)

var currentTimeMs int64
var currentTimeNs int64

func init() {
	DoTickerSchedule(context.Background(), func() {
		atomic.StoreInt64(&currentTimeMs, time.Now().Unix())
		atomic.StoreInt64(&currentTimeNs, time.Now().UnixNano())
	}, time.Duration(1)*time.Millisecond)
}

func GetCurrentTimeMs() int64 {
	return atomic.LoadInt64(&currentTimeMs)
}

func GetCurrentTimeNs() int64 {
	return atomic.LoadInt64(&currentTimeNs)
}
