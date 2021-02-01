// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"
)

const LogPrefix = "%s %s %s=>%d : "

type LogLevel int32

const (
	Trace LogLevel = iota
	Debug
	Info
	Warn
	Error
)

const (
	ErrorLevel = "[error]"
	WarnLevel  = "[warn]"
	InfoLevel  = "[info]"
	DebugLevel = "[debug]"
	TraceLevel = "[trace]"

	TimeFormatStr = "2006-01-02 15:04:05"
)

var RpcLog Logger = NewTestLogger("lraft-test")

//InitLogger 初始化一个默认的 Logger
func InitLogger(baseDir, name string) (err error) {
	filePath = filepath.Join(baseDir, "logs")
	if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
		return err
	}
	filePath = baseDir
	RpcLog, err = NewLogger(name)
	return err
}

//ResetGlobalLogger 重新设置 Logger
func ResetGlobalLogger(logger Logger) {
	RpcLog = logger
}

type Logger interface {
	SetLevel(level LogLevel)

	Debug(format string, args ...interface{})

	Info(format string, args ...interface{})

	Warn(format string, args ...interface{})

	Error(format string, args ...interface{})

	Trace(format string, args ...interface{})

	Close()

	Sink() LogSink
}

type AbstractLogger struct {
	name         string
	sink         LogSink
	level        LogLevel
	logEventChan chan LogEvent
	ctx          context.Context
	isClose      int32
}

var filePath string

//NewTestLogger 构建测试用的 Logger, 打印在控制台上
func NewTestLogger(name string) Logger {
	l := &AbstractLogger{
		name:         name,
		sink:         &ConsoleLogSink{},
		logEventChan: make(chan LogEvent, 16384),
		ctx:          context.Background(),
	}

	l.start()
	return l
}

func NewLogger(name string) (Logger, error) {
	f, err := os.OpenFile(filepath.Join(filePath, name+".log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	l := &AbstractLogger{
		name: name,
		sink: &FileLogSink{
			logger: log.New(f, "", log.Lmsgprefix),
		},
		logEventChan: make(chan LogEvent, 16384),
		ctx:          context.Background(),
	}

	l.start()
	return l, nil
}

//NewLoggerWithSink 构建一个 Logger, 但是日志的真实输出的 LogSink 可以自定义实现
func NewLoggerWithSink(name string, sink LogSink) Logger {
	l := &AbstractLogger{
		name: name,
		sink: sink,
		ctx:  context.Background(),
	}

	l.start()
	return l
}

func (l *AbstractLogger) SetLevel(level LogLevel) {
	atomic.StoreInt32((*int32)(&l.level), int32(level))
}

func (l *AbstractLogger) Trace(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.logEventChan <- LogEvent{
		Level:  Trace,
		Format: format,
		Args:   convertToLogArgs(TraceLevel, time.Now().Format(TimeFormatStr), args...),
	}
}

func (l *AbstractLogger) Debug(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.canLog(Debug, func() {
		l.logEventChan <- LogEvent{
			Level:  Debug,
			Format: format,
			Args:   convertToLogArgs(DebugLevel, time.Now().Format(TimeFormatStr), args...),
		}
	})
}

func (l *AbstractLogger) Info(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.canLog(Info, func() {
		l.logEventChan <- LogEvent{
			Level:  Info,
			Format: format,
			Args:   convertToLogArgs(InfoLevel, time.Now().Format(TimeFormatStr), args...),
		}
	})
}

func (l *AbstractLogger) Warn(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.canLog(Warn, func() {
		l.logEventChan <- LogEvent{
			Level:  Warn,
			Format: format,
			Args:   convertToLogArgs(WarnLevel, time.Now().Format(TimeFormatStr), args...),
		}
	})
}

func (l *AbstractLogger) Error(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.canLog(Error, func() {
		l.logEventChan <- LogEvent{
			Level:  Error,
			Format: format,
			Args:   convertToLogArgs(ErrorLevel, time.Now().Format(TimeFormatStr), args...),
		}
	})
}

func (l *AbstractLogger) canLog(level LogLevel, print func()) {
	if atomic.LoadInt32((*int32)(&l.level)) <= int32(level) {
		print()
	}
}

func (l *AbstractLogger) Close() {
	atomic.StoreInt32(&l.isClose, 1)
	l.ctx.Done()
	close(l.logEventChan)
}

func (l *AbstractLogger) Sink() LogSink {
	return l.sink
}

func (l *AbstractLogger) start() {
	go func(ctx context.Context) {
		for {
			var e LogEvent
			select {
			case e = <-l.logEventChan:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case <-ctx.Done():
				return
			}
		}
	}(l.ctx)
}

type LogEvent struct {
	Level  LogLevel
	Format string
	Args   []interface{}
}

type LogSink interface {
	OnEvent(name string, level LogLevel, format string, args ...interface{})
}

type ConsoleLogSink struct {
}

func (fl *ConsoleLogSink) OnEvent(name string, level LogLevel, format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}

type FileLogSink struct {
	logger *log.Logger
}

func (fl *FileLogSink) OnEvent(name string, level LogLevel, format string, args ...interface{}) {
	fl.logger.Printf(format, args...)
}

// 重新构建日志参数
func convertToLogArgs(level, time string, args ...interface{}) []interface{} {
	a := make([]interface{}, len(args)+4)
	a[0] = level
	a[1] = time
	a[2], a[3] = GetCaller(4)
	if args != nil {
		for i := 4; i < len(a); i++ {
			a[i] = args[i-4]
		}
	}
	return a
}

func GetCaller(depth int) (string, int) {
	_, file, line, _ := runtime.Caller(depth)
	return file, line
}
