package core

import "sync"

type ReplicatorGroup struct {
	lock		sync.Mutex
	replicators	map[string]*Replicator
}

