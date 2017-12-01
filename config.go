package raft

import (
	"math/rand"
	"time"
)

type Config struct {
	HeartBeatInterval   int64
	ElectionMinTimeout  int64
	ElectionRandTimeout int64

	// when the length of lastApplied log entry in entries(in raft state) reachs LogEntriesmaxSize, raft should take a snapshot.
	LogEntriesMaxSize uint64

	WorkDir string

	Peers []*Node
}

func (c *Config) randElectionTimeout() int64 {
	rand.Seed(time.Now().UnixNano())
	return c.ElectionMinTimeout + rand.Int63n(c.ElectionRandTimeout)
}

// DefaultConfig sets heartbeatInterval 20ms, electionMinTimeout 150ms-300ms
var DefaultConfig = &Config{
	HeartBeatInterval:   20,
	ElectionMinTimeout:  150,
	ElectionRandTimeout: 150,
	LogEntriesMaxSize:   4096,
	WorkDir:             "/tmp/",
}
