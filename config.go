package raft

import (
	"math/rand"
	"time"
)

type Config struct {
	HeartBeatInterval   int64
	ElectionMinTimeout  int64
	ElectionRandTimeout int64
}

func (c *Config) randElectionTimeout() int64 {
	rand.Seed(time.Now().UnixNano())
	return c.ElectionMinTimeout + rand.Int63n(c.ElectionRandTimeout)
}
