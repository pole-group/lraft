package utils

import (
	"context"
	"time"

	"github.com/jjeffcaii/reactor-go/scheduler"
)

var goroutinePool = scheduler.NewElastic(8)

func RunInGoroutine(ctx context.Context, runnable func(ctx context.Context))  {
	goroutinePool.Worker().Do(func() {
		runnable(ctx)
	})
}

func NewGoroutine(ctx context.Context, runnable func(ctx context.Context)) {
	go runnable(ctx)
}

func DoTimerScheduleByOne(ctx context.Context, work func(), delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)

		for {
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
				work()
			}
		}
	}()

}

func DoTimerSchedule(ctx context.Context, work func(), delay time.Duration, supplier func() time.Duration) {
	go func() {
		timer := time.NewTimer(delay)

		for {
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
				work()
				timer.Reset(supplier())
			}
		}

	}()

}

func DoTickerSchedule(ctx context.Context, work func(), delay time.Duration) {
	go func() {
		ticker := time.NewTicker(delay)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
			case <-ticker.C:
				work()
			}
		}

	}()

}
