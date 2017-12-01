package raft

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const Debug = 0

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug > level {
		log.Printf(format, a...)
	}
	return
}

func makeServers(n int) (res []*Raft, applies []chan ApplyMsg) {
	var nodes []*Node
	for i := 0; i < n; i++ {
		tmp := &Node{
			ID:   uint64(i),
			Addr: ":" + strconv.Itoa(10086+int(i)),
		}
		nodes = append(nodes, tmp)
	}
	for i := 0; i < n; i++ {
		var tmpApply = make(chan ApplyMsg, 10)
		var tmp = Make(DefaultConfig, nodes, nodes[i], NewMemoryStorage(tmpApply), tmpApply)
		applies = append(applies, tmpApply)
		res = append(res, tmp)
	}

	return
}

func findLeader(t *testing.T, rfs []*Raft) int {
	count := 0
Loop:
	for i, v := range rfs {
		v.mu.Lock()
		if v.me.leader == int64(v.me.ID) {
			//		t.Logf("find leader:%v\n", v.me.leader)
			v.mu.Unlock()
			return i
		}
		v.mu.Unlock()
	}
	count++
	// may be leader election is not complete, try 3 times.
	if count < 3 {
		goto Loop
	}

	t.Errorf("not find leader...\n")
	return 0
}

func stopLeader(t *testing.T, rfs []*Raft) {
	index := findLeader(t, rfs)
	rfs[index].Kill()
}

func stopServers(rfs []*Raft) {
	for _, v := range rfs {
		v.Kill()
	}
}

var recordLeader = new(leaderRecord)

type leaderRecord struct {
	mu sync.Mutex
	r  map[uint64][]uint64 // one term map to leaders
}

// reset test enviroment
func reset(t *testing.T) {
	t.Logf("wait one second to reset environments...\n")
	time.Sleep(time.Second)
	removeTmpPersisterFile(t)
	recordLeader.reset()
}
func (lr *leaderRecord) reset() {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.r = make(map[uint64][]uint64)
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

func (lr *leaderRecord) checkLeader(t *testing.T) {
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

func removeTmpPersisterFile(t *testing.T) {
	d, err := os.Open(DefaultConfig.WorkDir)
	if err != nil {
		t.Errorf("reset tmp dir failed:%v\n", err)
	}
	defer d.Close()

	names, _ := d.Readdirnames(-1)
	for _, name := range names {
		if strings.HasSuffix(name, "raft") {
			err = os.Remove(DefaultConfig.WorkDir + name)
			if err != nil {
				t.Errorf("remove tmp file failed:%v\n", err)
			}
		}
	}
}
