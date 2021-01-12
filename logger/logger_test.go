package logger

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pole-group/lraft/utils"
)

type TestFileSink struct {
	sink     *FileLogSink
	observer func()
}

func NewTestFileSink(name string) *TestFileSink {
	f, err := os.OpenFile(filepath.Join(filePath, name+".log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	utils.CheckErr(err)
	log := log.New(f, "", log.Lmsgprefix)
	return &TestFileSink{
		sink: &FileLogSink{
			logger: log,
		},
	}
}

func (fl *TestFileSink) OnEvent(name string, level LogLevel, format string, args ...interface{}) {
	fl.sink.OnEvent(name, level, format, args)
	fl.observer()
}

func Test_RaftLogger_Info(t *testing.T) {
	latch := &sync.WaitGroup{}
	latch.Add(1)

	sink := NewTestFileSink("lessspring_test")
	sink.observer = func() {
		latch.Done()
	}

	logger := NewRaftLoggerWithSink("lessspring_test", sink)
	logger.Info("this is test %s", time.Now().Format(TimeFormatStr))

	f, err := os.Open(filepath.Join(filePath, "lessspring_test.log"))
	utils.CheckErr(err)

	reader := bufio.NewReader(f)
	b := make([]byte, reader.Size())
	reader.Read(b)
	fmt.Printf(string(b))

	latch.Wait()
}
