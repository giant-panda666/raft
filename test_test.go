package raft

import (
	//	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

var DefaultConfig = &Config{
	HeartBeatInterval:   20,
	ElectionMinTimeout:  150,
	ElectionRandTimeout: 200,
}

var stopMu sync.Mutex
var stopRaftServer bool

func makeServers(n int) {
	var nodes []*Node
	for i := 0; i < n; i++ {
		tmp := &Node{
			ID:   uint64(i),
			Addr: ":" + strconv.Itoa(10086+int(i)),
		}
		nodes = append(nodes, tmp)
	}
	for i := 0; i < n; i++ {
		var _ = Make(DefaultConfig, nodes, nodes[i], make(chan ApplyMsg))
	}
}

var recordLeader = new(leaderRecord)

type leaderRecord struct {
	mu sync.Mutex
	r  map[uint64][]uint64 // one term map to leaders
}

func (lr *leaderRecord) record(rf *Raft) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	if lr.r == nil {
		lr.r = make(map[uint64][]uint64)
	}

	leaders, ok := lr.r[rf.me.curTerm]
	if !ok {
		leaders = make([]uint64, 0)
	}
	leaders = append(leaders, rf.me.ID)
	lr.r[rf.me.curTerm] = leaders
}

func (lr *leaderRecord) isValid(t *testing.T) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	var flag bool
	for k, v := range lr.r {
		flag = true
		if len(v) > 1 {
			t.Errorf("InitialElection failed during term:%v, leaders:%v\n", k, v)
		}
		t.Logf("term:%v, leader is:%v\n", k, v)
	}
	if flag {
		t.Logf("passed...\n")
	} else {
		t.Errorf("no leader is elected...")
	}
}

///////////////////// leader election ////////////
func TestInitialElection(t *testing.T) {
	makeServers(4)
	time.Sleep(1 * time.Second)
	recordLeader.isValid(t)

	stopMu.Lock()
	stopRaftServer = true
	stopMu.Unlock()
}
