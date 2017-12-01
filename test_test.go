package raft

import (
	//	"log"

	pb "raft/raftpb"
	"testing"
	"time"

	"golang.org/x/net/context"
)

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

///////////////////// persister raft state ////////////
func TestPersistRaftState(t *testing.T) {
	reset(t)

	server0, _ := makeServers(2)
	//	DPrintf(0, "0:%v, 1:%v\n", server0[0].me.commitIndex, server0[1].me.commitIndex)
	time.Sleep(time.Second)
	//	DPrintf(0, "after Sleep\n")
	leaderIndex := findLeader(t, server0)
	//	DPrintf(0, "after findLeader\n")
	server0[leaderIndex].Propose(context.Background(), &pb.ProposeArgs{Data: []byte("helloworld")})
	lenentries := len(server0[leaderIndex].me.entries)
	//time.Sleep(time.Second) // wait propose complete
	stopServers(server0)
	time.Sleep(2 * time.Second)

	server1, _ := makeServers(2)
	//	DPrintf(0, "0:%v, 1:%v\n", server1[0].me.commitIndex, server1[1].me.commitIndex)
	time.Sleep(time.Second)
	if len(server1[leaderIndex].me.entries) != lenentries {
		t.Errorf("persister raft state failed...\nlen(entries):%v, after restart\nlen(entries):%v\n", lenentries, len(server1[leaderIndex].me.entries))
	}
	t.Logf("persist raft state passed...\n")
	stopServers(server1)
}

///////////////////// snapshot ////////////
func TestSnapshot(t *testing.T) {
	reset(t)

	node0 := &Node{
		ID:         0,
		Addr:       ":10086",
		nextIndex:  make([]uint64, 2),
		matchIndex: make([]uint64, 2),
	}
	node1 := &Node{
		ID:         1,
		Addr:       ":10087",
		nextIndex:  make([]uint64, 2),
		matchIndex: make([]uint64, 2),
	}
	peers := []*Node{node0, node1}
	node0.nextIndex[1] = 2

	raft0 := &Raft{}
	raft1 := &Raft{}
	raft0.me = node0
	raft1.me = node1
	raft0.heartbeatChan = make(chan struct{}, 16)
	raft1.heartbeatChan = make(chan struct{}, 16)
	raft0.commitChan = make(chan struct{})
	raft1.commitChan = make(chan struct{})
	applyMsg0, applyMsg1 := make(chan ApplyMsg), make(chan ApplyMsg)
	s0 := NewMemoryStorage(applyMsg0)
	s0.data = append(s0.data, []byte("hello"), []byte("world"))
	s0.lastAppliedIndex = 2
	s0.lastAppliedTerm = 2
	s0.Save()
	s1 := NewMemoryStorage(applyMsg1)
	raft0.storage = s0
	raft1.storage = s1
	raft0.me.peers = peers
	raft1.me.peers = peers
	raft0.me.curState = leaderState
	raft1.me.curState = followerState
	raft0.me.leader = 0
	raft1.me.leader = 0
	raft0.me.curTerm = 3
	raft1.me.curTerm = 3
	raft1.me.persister = newPersister(1, "/tmp/")

	raft0.me.entries = append(raft0.me.entries, &pb.Entry{Type: pb.EntryType_Normal, Term: 2, Index: 2})
	raft0.me.commitIndex = 2
	raft0.isDuringSnap = make([]bool, 2)
	raft1.me.entries = append(raft1.me.entries, &pb.Entry{Type: pb.EntryType_Normal, Term: 0, Index: 0})
	go raft0.registerRPCServer()
	go raft1.registerRPCServer()
	time.Sleep(time.Second)

	raft0.broadcastAppendEntries()
	time.Sleep(time.Second)
	if len(s1.data) != 2 || string(s1.data[0]) != "hello" || string(s1.data[1]) != "world" {
		t.Errorf("install snapshot failed...\n")
	}
	t.Logf("install snapshot passed...")
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
	var args []*pb.ProposeArgs
	for i := 0; i < 3; i++ {
		args = append(args, &pb.ProposeArgs{Data: []byte(msg[i])})
	}
	time.Sleep(time.Second)
	leaderIndex := findLeader(t, servers)
	t.Logf("leaderIndex:%v\n", leaderIndex)

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
	var followerIndex int
	for i := 0; i < 3; i++ {
		if followerIndex == leaderIndex {
			followerIndex++
		} else {
			break
		}
	}
	t.Logf("followerIndex:%v\n", followerIndex)

	reply, err = servers[followerIndex].Propose(context.Background(), args[followerIndex])
	if err != nil {
		t.Errorf("Propose RPC error:%s\n", err.Error())
	}
	if reply.Success != false || reply.ErrCode != ErrNotLeaderCode || reply.ErrMsg != ErrNotLeader.Error() || reply.Addr != servers[leaderIndex].me.Addr {
		t.Errorf("send log entry to follower failed...propose returns false...reply:%v\n", *reply)
	} else {
		t.Logf("send log entry to follower passed...\n")
	}

	// ---------------------send log entry to candidate---------------------------
	servers[followerIndex].Kill()
	servers[leaderIndex].Kill()
	t.Logf("kill leader and one follower, wait 1 sencond another follower to be candidate...\n")
	time.Sleep(time.Second)
	var candidateIndex int
	for i := 0; i < 3; i++ {
		if candidateIndex == leaderIndex || candidateIndex == followerIndex {
			candidateIndex++
		} else {
			break
		}
	}
	t.Logf("candidateIndex:%v\n", candidateIndex)

	reply, err = servers[candidateIndex].Propose(context.Background(), args[candidateIndex])
	if err != nil {
		t.Errorf("Propose RPC error:%s\n", err.Error())
	}
	if reply.Success != false || reply.ErrCode != ErrNotReadyCode || reply.ErrMsg != ErrNotReady.Error() || reply.Addr != "" {
		t.Errorf("send log entry to candidate failed...propose returns false...reply:%v\n", *reply)
	} else {
		t.Logf("send log entry to candidate passed...\n")
	}
	servers[candidateIndex].Kill()
}
