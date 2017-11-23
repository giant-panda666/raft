package raft

import (
	//	"log"
	"os"
	pb "raft/raftpb"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
)

var stopMu sync.Mutex
var stopRaftServer bool

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
		var tmp = Make(DefaultConfig, nodes, nodes[i], tmpApply)
		applies = append(applies, tmpApply)
		res = append(res, tmp)
	}

	return
}

func findLeader(t *testing.T, rfs []*Raft) int {
	for i, v := range rfs {
		v.mu.Lock()
		if v.me.leader == int64(v.me.ID) {
			t.Logf("find leader:%v\n", v.me.leader)
			v.mu.Unlock()
			return i
		}
		v.mu.Unlock()
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

///////////////////// leader election ////////////
func TestInitialElection(t *testing.T) {
	reset(t)

	servers, _ := makeServers(3)
	time.Sleep(time.Second)

	stopLeader(t, servers)
	recordLeader.checkLeader(t)

	time.Sleep(time.Second)
	stopServers(servers)
	recordLeader.checkLeader(t)
}

///////////////log replication/////////////
func TestLogReplication(t *testing.T) {
	reset(t)

	servers, applies := makeServers(3)
	var msg = []string{
		"hello",
		"world",
		"helloworld",
	}
	var args = []*pb.ProposeArgs{
		&pb.ProposeArgs{
			Data: []byte(msg[0]),
		},
		&pb.ProposeArgs{
			Data: []byte(msg[1]),
		},
		&pb.ProposeArgs{
			Data: []byte(msg[2]),
		},
	}
	time.Sleep(time.Second)
	leaderIndex := findLeader(t, servers)

	// --------------send log entry to leader, reply should be ok-------------------
	reply, err := servers[leaderIndex].Propose(context.Background(), args[leaderIndex])
	if err != nil {
		t.Errorf("Propose RPC error:%s\n", err.Error())
	}
	if reply.Success != true {
		t.Errorf("send log entry to leader failed...propose returns false...reply:%v\n", *reply)
	}
	// log should be in every raft server's log entries
	time.Sleep(time.Second)
	for i, v := range servers {
		t.Logf("server:%v, entries:%v\n", i, v.me.entries)
	}
	// log should be applied in every raft server
	for i := 0; i < 3; i++ {
		select {
		case msg := <-applies[i]:
			if string(msg.Command) == string(args[leaderIndex].Data) {
				t.Logf("server:%v, apply msg same, pass...\n", i)
			} else {
				t.Errorf("apply msg is not same, applied:%v, it should be:%v\n", string(msg.Command), string(args[leaderIndex].Data))
			}
		case <-time.After(time.Second * 2):
			t.Errorf("after 2 second, not receive apply msg...\n")
		}
	}

	// ---------------------send log entry to follower----------------------------
	stopLeader(t, servers)
	time.Sleep(time.Second)
	newLeaderIndex := findLeader(t, append(servers[:leaderIndex], servers[leaderIndex+1:]...))
	var followerIndex int
	for i := 0; i < 3; i++ {
		if i != leaderIndex && i != newLeaderIndex {
			followerIndex = i
		}
	}
	t.Logf("leaderIndex:%v, newLeaderIndex:%v, followerIndex:%v\n", leaderIndex, newLeaderIndex, followerIndex)
	reply, err = servers[followerIndex].Propose(context.Background(), args[followerIndex])
	if err != nil {
		t.Errorf("Propose RPC error:%s\n", err.Error())
	}
	if reply.Success != false || reply.ErrCode != ErrNotLeaderCode || reply.ErrMsg != ErrNotLeader.Error() || reply.Addr != servers[newLeaderIndex].me.Addr {
		t.Errorf("send log entry to follower failed...propose returns false...reply:%v\n", *reply)
	}
	t.Logf("send log entry to follower passed...\n")

	// ---------------------send log entry to candidate---------------------------
	servers[newLeaderIndex].Kill()
	time.Sleep(time.Second)
	candidateIndex := followerIndex
	reply, err = servers[candidateIndex].Propose(context.Background(), args[candidateIndex])
	if err != nil {
		t.Errorf("Propose RPC error:%s\n", err.Error())
	}
	if reply.Success != false || reply.ErrCode != ErrNotReadyCode || reply.ErrMsg != ErrNotReady.Error() || reply.Addr != "" {
		t.Errorf("send log entry to candidate failed...propose returns false...reply:%v\n", *reply)
	}
	t.Logf("send log entry to candidate passed...\n")

	// all server exit, clean persister file
	servers[candidateIndex].Kill()
}

///////////////////// persister raft state ////////////
func TestPersistRaftState(t *testing.T) {
	reset(t)

	server0, _ := makeServers(2)
	time.Sleep(time.Second)
	leaderIndex := findLeader(t, server0)
	server0[leaderIndex].Propose(context.Background(), &pb.ProposeArgs{Data: []byte("helloworld")})
	lenentries := len(server0[leaderIndex].me.entries)
	stopServers(server0)
	time.Sleep(2 * time.Second)

	server1, _ := makeServers(2)
	time.Sleep(time.Second)
	if len(server1[leaderIndex].me.entries) != lenentries {
		t.Errorf("persister raft state failed...\nlen(entries):%v, after restart\nlen(entries):%v\n", lenentries, len(server1[leaderIndex].me.entries))
	}
	t.Logf("persist raft state passed...\n")
}
