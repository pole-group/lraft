package logger

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"lraft/utils"
)

const LogPrefix = "%s-%s : "

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

type Logger interface {
	Debug(format string, args ...interface{})

	Info(format string, args ...interface{})

	Warn(format string, args ...interface{})

	Error(format string, args ...interface{})

	Trace(format string, args ...interface{})

	Close()

	Sink() LogSink
}

type lRaftLogger struct {
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

func LogInit(baseDir string) {
	filePath = baseDir
}

func NewLRaftLogger(name string) Logger {
	f, err := os.OpenFile(filepath.Join(filePath, name+".log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	utils.CheckErr(err)
	l := &lRaftLogger{
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

func NewLRaftLoggerWithSink(name string, sink LogSink) Logger {
	l := &lRaftLogger{
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

func (l *lRaftLogger) Debug(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.debugBuf <- LogEvent{
		Level:  Debug,
		Format: format,
		Args:   args,
	}
}

func (l *lRaftLogger) Info(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.infoBuf <- LogEvent{
		Level:  Info,
		Format: format,
		Args:   args,
	}
}

func (l *lRaftLogger) Warn(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.warnBuf <- LogEvent{
		Level:  Warn,
		Format: format,
		Args:   args,
	}
}

func (l *lRaftLogger) Error(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.errorBuf <- LogEvent{
		Level:  Error,
		Format: format,
		Args:   args,
	}
}

func (l *lRaftLogger) Trace(format string, args ...interface{}) {
	if atomic.LoadInt32(&l.isClose) == 1 {
		return
	}
	l.traceBuf <- LogEvent{
		Level:  Trace,
		Format: format,
		Args:   args,
	}
}

func (l *lRaftLogger) Close() {
	atomic.StoreInt32(&l.isClose, 1)
	l.ctx.Done()
	close(l.debugBuf)
	close(l.infoBuf)
	close(l.warnBuf)
	close(l.errorBuf)
	close(l.traceBuf)
}

func (l *lRaftLogger) Sink() LogSink {
	return l.sink
}

func (l *lRaftLogger) start() {
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
