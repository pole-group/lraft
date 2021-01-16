package logger

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pole-group/lraft/utils"
)

const LogPrefix = "%s-%s-%s : "

type LogLevel int

const (
	Error LogLevel = iota
	Warn
	Info
	Debug
	Trace
)

const (
	ErrorLevel = "[error]"
	WarnLevel  = "[warn]"
	InfoLevel  = "[info]"
	DebugLevel = "[debug]"
	TraceLevel = "[trace]"

	TimeFormatStr = "2006-01-02 15:04:05"
)

var GlobalRaftLog Logger = NewTestRaftLogger("lraft-test")

func InitLRaftLogger(baseDir, name string) {
	filePath = baseDir
	GlobalRaftLog = NewRaftLogger(name)
}

type Logger interface {
	Debug(format string, args ...interface{})

	Info(format string, args ...interface{})

	Warn(format string, args ...interface{})

	Error(format string, args ...interface{})

	Trace(format string, args ...interface{})

	Close()

	Sink() LogSink
}

type raftLogger struct {
	name     string
	sink     LogSink
	debugBuf chan LogEvent
	infoBuf  chan LogEvent
	warnBuf  chan LogEvent
	errorBuf chan LogEvent
	traceBuf chan LogEvent
	ctx      context.Context
	isClose  int32
}

var filePath string

func init() {
	baseDir, err := utils.Home()
	utils.CheckErr(err)
	filePath = filepath.Join(baseDir, "logs")
	utils.CheckErr(os.MkdirAll(filePath, os.ModePerm))
}

func NewTestRaftLogger(name string) Logger {
	l := &raftLogger{
		name: name,
		sink: &ConsoleLogSink{},
		debugBuf: make(chan LogEvent, 1024),
		infoBuf:  make(chan LogEvent, 1024),
		warnBuf:  make(chan LogEvent, 1024),
		errorBuf: make(chan LogEvent, 1024),
		traceBuf: make(chan LogEvent, 1024),
		ctx:      context.Background(),
	}

	l.start()
	return l
}

func NewRaftLogger(name string) Logger {
	f, err := os.OpenFile(filepath.Join(filePath, name+".log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	utils.CheckErr(err)
	l := &raftLogger{
		name: name,
		sink: &FileLogSink{
			logger: log.New(f, "", log.Lmsgprefix),
		},
		debugBuf: make(chan LogEvent, 1024),
		infoBuf:  make(chan LogEvent, 1024),
		warnBuf:  make(chan LogEvent, 1024),
		errorBuf: make(chan LogEvent, 1024),
		traceBuf: make(chan LogEvent, 1024),
		ctx:      context.Background(),
	}

	l.start()
	return l
}

func NewRaftLoggerWithSink(name string, sink LogSink) Logger {
	l := &raftLogger{
		name:     name,
		sink:     sink,
		debugBuf: make(chan LogEvent, 1024),
		infoBuf:  make(chan LogEvent, 1024),
		warnBuf:  make(chan LogEvent, 1024),
		errorBuf: make(chan LogEvent, 1024),
		traceBuf: make(chan LogEvent, 1024),
		ctx:      context.Background(),
	}

	l.start()
	return l
}

func (l *raftLogger) Debug(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.debugBuf <- LogEvent{
		Level:  Debug,
		Format: format,
		Args:   convertToLogArgs(args),
	}
}

func (l *raftLogger) Info(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.infoBuf <- LogEvent{
		Level:  Info,
		Format: format,
		Args:   convertToLogArgs(args),
	}
}

func (l *raftLogger) Warn(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.warnBuf <- LogEvent{
		Level:  Warn,
		Format: format,
		Args:   convertToLogArgs(args),
	}
}

func (l *raftLogger) Error(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.errorBuf <- LogEvent{
		Level:  Error,
		Format: format,
		Args:   convertToLogArgs(args),
	}
}

func (l *raftLogger) Trace(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.traceBuf <- LogEvent{
		Level:  Trace,
		Format: format,
		Args:   convertToLogArgs(args),
	}
}

func (l *raftLogger) Close() {
	atomic.StoreInt32(&l.isClose, 1)
	l.ctx.Done()
	close(l.debugBuf)
	close(l.infoBuf)
	close(l.warnBuf)
	close(l.errorBuf)
	close(l.traceBuf)
}

func (l *raftLogger) Sink() LogSink {
	return l.sink
}

func (l *raftLogger) start() {
	utils.NewGoroutine(l.ctx, func(ctx context.Context) {
		for {
			var e LogEvent
			select {
			case e = <-l.debugBuf:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case e = <-l.infoBuf:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case e = <-l.warnBuf:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case e = <-l.errorBuf:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case e = <-l.traceBuf:
				l.sink.OnEvent(l.name, e.Level, LogPrefix+e.Format, e.Args...)
			case <-ctx.Done():
				return
			}
		}
	})
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
	timeStr := time.Now().Format(TimeFormatStr)
	switch level {
	case Debug:
		fmt.Printf(format + "\n", timeStr, DebugLevel, args)
	case Info:
		fmt.Printf(format + "\n", timeStr, InfoLevel, args)
	case Warn:
		fmt.Printf(format + "\n", timeStr, WarnLevel, args)
	case Error:
		fmt.Printf(format + "\n", timeStr, ErrorLevel, args)
	case Trace:
		fmt.Printf(format + "\n", timeStr, TraceLevel, args)
	default:
		// do nothing
	}
}


type FileLogSink struct {
	logger *log.Logger
}

func (fl *FileLogSink) OnEvent(name string, level LogLevel, format string, args ...interface{}) {
	timeStr := time.Now().Format(TimeFormatStr)
	switch level {
	case Debug:
		fl.logger.Printf(format, timeStr, DebugLevel, args)
	case Info:
		fl.logger.Printf(format, timeStr, InfoLevel, args)
	case Warn:
		fl.logger.Printf(format, timeStr, WarnLevel, args)
	case Error:
		fl.logger.Printf(format, timeStr, ErrorLevel, args)
	case Trace:
		fl.logger.Printf(format, timeStr, TraceLevel, args)
	default:
		// do nothing
	}
}

// 重新构建日志参数
func convertToLogArgs(args []interface{}) []interface{} {
	a := make([]interface{}, len(args)+2)
	a[0], a[1] = GetCaller(4)
	if args != nil {
		for i := 4; i < len(a); i++ {
			a[i] = args[i-2]
		}
	}
	return a
}

func GetCaller(depth int) (string, int) {
	_, file, line, _ := runtime.Caller(depth)
	return file, line
}
