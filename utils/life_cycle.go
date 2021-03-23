package utils

type LifeCycle interface {
	Init(args interface{}) error

	Shutdown()
}
