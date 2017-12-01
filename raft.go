package raft

import (
	"errors"
	"log"
	"net"
	pb "raft/raftpb"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"golang.org/x/net/context"
)

const snapshotChunkSize = 2 << 31

type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

type Raft struct {
	// Lock to protect shared access to this peer's state
	mu sync.Mutex
	// current raft server
	me *Node
	// config
	config *Config

	server *grpc.Server

	heartbeatChan   chan struct{}
	voteGrantedChan chan struct{}
	winVoteCampaign chan struct{}

	commitChan chan struct{}

	exited   bool
	exitChan chan struct{}
	waitExit chan struct{}

	snapMu       sync.Mutex
	isDuringSnap []bool
	storage      Storage
}

func (rf *Raft) RequestVotes(ctx context.Context, args *pb.RequestVotesArgs) (*pb.RequestVotesReply, error) {
	//	DPrintf(0, "RequestVotes args:%v\n", args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var err error
	var reply = &pb.RequestVotesReply{
		Term:        rf.me.curTerm,
		VoteGranted: false,
	}

	if rf.me.curTerm > args.Term || (rf.me.curTerm == args.Term && rf.me.votedFor == args.CandidateID) {
		return reply, err
	} else if rf.me.curTerm < args.Term {
		DPrintf(1, "RequestVotes, curTerm < args.Term, before ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
		rf.me.updateCurTerm(args.Term)
		rf.updateStateTo(followerState)
		rf.me.savePersistState()
		reply.Term = args.Term
		DPrintf(1, "RequestVotes, curTerm < args.Term, after ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
	}

	if rf.me.votedFor == none {
		if args.LastLogTerm < rf.me.lastLogTerm() {
			return reply, err
		}
		if args.LastLogTerm == rf.me.lastLogTerm() && args.LastLogIndex < rf.me.lastLogIndex() {
			return reply, err
		}
		reply.VoteGranted = true
		rf.updateStateTo(followerState)
		rf.me.grantVote(args.CandidateID)
		rf.me.savePersistState()
		DPrintf(1, "RequestVotes, voteFor:%v, ID:%v, term:%v, curState:%v\n", args.CandidateID, rf.me.ID, rf.me.curTerm, rf.me.curState)
		rf.voteGrantedChan <- struct{}{}
	}

	return reply, err
}

func (rf *Raft) updateStateTo(st stateType) {
	rf.me.updateStateTo(st)
}

func (rf *Raft) sendRequestVote(peer uint64, args *pb.RequestVotesArgs) (reply *pb.RequestVotesReply, err error) {
	conn, err := grpc.Dial(rf.me.peers[peer].Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect:%v\n", err)
	}

	c := pb.NewRaftClient(conn)
	reply, err = c.RequestVotes(context.Background(), args)
	return
}

func (rf *Raft) doRequestVote(peer uint64, args *pb.RequestVotesArgs) {
	reply, err := rf.sendRequestVote(peer, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.me.curState != candidateState || rf.me.curTerm != args.Term {
		return
	} else if rf.me.curTerm < reply.Term {
		rf.me.updateCurTerm(reply.Term)
		rf.updateStateTo(followerState)
		rf.me.savePersistState()
	} else if reply.VoteGranted {
		rf.me.voteCounter++
		if rf.me.voteCounter > uint64(len(rf.me.peers)/2) && rf.me.curState == candidateState {
			//	DPrintf(0, "////////////////////leader:%v, vote:%v, uint64(len(rf.me.peers)/2):%v\n", rf.me.ID, rf.me.voteCounter, uint64(len(rf.me.peers)/2))
			rf.updateStateTo(leaderState)
			// for-testing
			recordLeader.record(rf)
			rf.winVoteCampaign <- struct{}{}
		}
	}
}

func (rf *Raft) broadcastRequestVotes() {
	rf.mu.Lock()
	args := &pb.RequestVotesArgs{
		CandidateID:  rf.me.ID,
		Term:         rf.me.curTerm,
		LastLogTerm:  rf.me.lastLogTerm(),
		LastLogIndex: rf.me.lastLogIndex(),
	}
	DPrintf(1, "len(peers):%v\n", len(rf.me.peers))
	rf.mu.Unlock()
	for i := range rf.me.peers {
		if uint64(i) != rf.me.ID {
			go rf.doRequestVote(uint64(i), args)
		}
	}
}

func (rf *Raft) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.me.savePersistState()

	reply = &pb.AppendEntriesReply{
		Term:    rf.me.curTerm,
		Success: false,
	}
	if args.Term < rf.me.curTerm {
		return
	}

	rf.heartbeatChan <- struct{}{}
	rf.me.leader = int64(args.LeaderID)

	if args.Term > rf.me.curTerm {
		rf.me.updateCurTerm(args.Term)
		rf.me.updateStateTo(followerState)
		reply.Term = args.Term
	}

	if args.Entries != nil {
		baseIndex := rf.me.baseIndex()
		// prevLogIndex not in current entries's or prevLogTerm not equall to the term in prevLogIndex
		if args.PrevLogIndex < baseIndex || args.PrevLogIndex > rf.me.lastLogIndex() ||
			rf.me.entries[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm {
			return
		}
		rf.me.entries = append(rf.me.entries[:args.PrevLogIndex+1-baseIndex], args.Entries...)
		//DPrintf(0, "me.entries:%v, args.Entries:%v\n", rf.me.entries, args.Entries)
	}
	reply.Success = true
	if args.LeaderCommit > rf.me.commitIndex {
		//	DPrintf(0, "follower:%v, leaderID:%v, LeaderCommit:%v, commitIndex:%v, args.Entries:%v\n", rf.me.ID, args.LeaderID, args.LeaderCommit, rf.me.commitIndex, args.Entries)
		rf.me.commitIndex = args.LeaderCommit
		rf.commitChan <- struct{}{}
	}

	return
}

func (rf *Raft) sendAppendEntry(peer uint64, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, err error) {
	conn, err := grpc.Dial(rf.me.peers[peer].Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect:%v\n", err)
	}

	c := pb.NewRaftClient(conn)
	reply, err = c.AppendEntries(context.Background(), args)
	return
}

func (rf *Raft) doAppendEntry(peer uint64, args *pb.AppendEntriesArgs) {
	//	DPrintf(0, "3 leader:%v, commitIndex:%v, entries:%v, peer:%v\n", rf.me.ID, rf.me.commitIndex, rf.me.entries, peer)
	reply, err := rf.sendAppendEntry(peer, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me.curState != leaderState || rf.me.curTerm != args.Term {
		return
	}
	if rf.me.curTerm < reply.Term {
		rf.me.curTerm = reply.Term
		rf.me.updateStateTo(followerState)
		rf.me.savePersistState()
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.me.nextIndex[peer] = args.Entries[len(args.Entries)-1].Index + 1
			rf.me.matchIndex[peer] = rf.me.nextIndex[peer] - 1
			//	DPrintf(0, "leader:%v, peer:%v, nextIndex:%v\n", rf.me.ID, peer, rf.me.nextIndex[peer])
		}
		return
	}

	if rf.me.nextIndex[peer] > 1 {
		rf.me.nextIndex[peer]--
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// commit log, update commitIndex
	//	DPrintf(0, "1 leader:%v, commitIndex:%v, entries:%v\n", rf.me.ID, rf.me.commitIndex, rf.me.entries)
	if rf.me.updateCommitIndex() {
		rf.commitChan <- struct{}{}
	}
	//	DPrintf(0, "2 leader:%v, commitIndex:%v, entries:%v\n", rf.me.ID, rf.me.commitIndex, rf.me.entries)

	args := pb.AppendEntriesArgs{
		Term:         rf.me.curTerm,
		LeaderID:     rf.me.ID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.me.commitIndex,
	}
	//DPrintf(0, "args:%v\n", args)

	for i := range rf.me.peers {
		if uint64(i) == rf.me.ID {
			continue
		}
		prevLogEntry := rf.me.prevLog(uint64(i))
		//	DPrintf(0, "prevLogEntry:%v\n", prevLogEntry)
		if prevLogEntry.Type == pb.EntryType_None {
			rf.snapMu.Lock()
			if rf.isDuringSnap[i] { // already in install snapshot
				rf.snapMu.Unlock()
				return
			}
			rf.isDuringSnap[i] = true
			rf.snapMu.Unlock()

			args := pb.InstallSnapshotArgs{
				Term:     rf.me.curTerm,
				LeaderID: rf.me.ID,
			}
			args.Data, args.LastIncludeIndex, args.LastIncludeTerm = rf.storage.Read()
			//	DPrintf(0, "args.LastIncludeIndex:%v, args.LastIncludeTerm:%v\n", args.LastIncludeIndex, args.LastIncludeTerm)

			go rf.doInstallSnapshot(uint64(i), &args)
			continue
		}
		// sends current entry to the end
		baseIndex := rf.me.baseIndex()
		args.PrevLogIndex = prevLogEntry.Index
		args.PrevLogTerm = prevLogEntry.Term
		args.Entries = make([]*pb.Entry, len(rf.me.entries[args.PrevLogIndex+1-baseIndex:]))
		copy(args.Entries, rf.me.entries[args.PrevLogIndex+1-baseIndex:])
		//	DPrintf(0, "baseIndex:%v, prevLogIndex:%v, PrevLogTerm:%v, rf.me.entries:%v, args.Entries:%v\n", baseIndex, args.PrevLogIndex, args.PrevLogTerm, rf.me.entries, args.Entries)
		go rf.doAppendEntry(uint64(i), &args)
	}
}

func (rf *Raft) truncateLog(lastIncludeIndex, lastIncludeTerm uint64) {
	index := -1
	entry0 := &pb.Entry{Term: lastIncludeTerm, Index: lastIncludeIndex}
	for i := len(rf.me.entries) - 1; i >= 0; i-- {
		if rf.me.entries[i].Index == lastIncludeIndex && rf.me.entries[i].Term == lastIncludeTerm {
			index = i
			break
		}
	}
	if index != -1 {
		rf.me.entries = append([]*pb.Entry{entry0}, rf.me.entries[index+1:]...)
	} else {
		rf.me.entries = []*pb.Entry{entry0}
	}
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (reply *pb.InstallSnapshotReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply = &pb.InstallSnapshotReply{
		Term: rf.me.curTerm,
	}

	//	DPrintf(0, "args.Term:%v, curTerm:%v, args.Data:%v\n", args.Term, rf.me.curTerm, len(args.Data))
	if args.Term < rf.me.curTerm {
		return
	}
	rf.heartbeatChan <- struct{}{}
	rf.me.leader = int64(args.LeaderID)

	if rf.me.curTerm < args.Term {
		rf.me.updateCurTerm(args.Term)
		rf.me.updateStateTo(followerState)
		reply.Term = args.Term
		rf.me.savePersistState()
	}

	rf.storage.Write(args.Data, args.LastIncludeIndex, args.LastIncludeTerm)
	rf.storage.Apply()

	rf.truncateLog(args.LastIncludeIndex, args.LastIncludeTerm)
	rf.me.lastApplied = args.LastIncludeIndex
	rf.me.commitIndex = args.LastIncludeIndex
	//	DPrintf(0, "lastApplied:%v, commitIndex:%v\n", rf.me.lastApplied, rf.me.commitIndex)
	rf.me.savePersistState()

	return
}

func (rf *Raft) sendInstallSnapshot(peer uint64, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	conn, err := grpc.Dial(rf.me.peers[peer].Addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect:%v\n", err)
	}

	c := pb.NewRaftClient(conn)
	reply, err := c.InstallSnapshot(context.Background(), args)
	return reply, err
}

func (rf *Raft) doInstallSnapshot(server uint64, args *pb.InstallSnapshotArgs) {
	reply, errRPC := rf.sendInstallSnapshot(server, args)
	if errRPC != nil {
		//	DPrintf(0, "errRPC:%v\n", errRPC)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		rf.snapMu.Lock()
		rf.isDuringSnap[server] = false
		rf.snapMu.Unlock()
	}()
	if rf.me.curState != leaderState || rf.me.curTerm != args.Term {
		return
	}
	if rf.me.curTerm < reply.Term {
		rf.me.updateCurTerm(reply.Term)
		rf.me.updateStateTo(followerState)
		rf.me.savePersistState()
		return
	}
	rf.me.nextIndex[server] = args.LastIncludeIndex + 1
	rf.me.matchIndex[server] = args.LastIncludeIndex
}

func (rf *Raft) Kill() {
	if rf.exited {
		return
	}
	rf.exited = true
	rf.exitChan <- struct{}{}
	<-rf.waitExit
}

func (rf *Raft) commitLogs() {
	for {
		select {
		case <-rf.commitChan:
			rf.mu.Lock()
			rf.me.commitLogs()
			DPrintf(0, "id:%v, commitIndex:%v, entries:%v\n", rf.me.ID, rf.me.commitIndex, rf.me.entries)
			// todo if lastapply-baseindex > 2048?, take a snapshot
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.exitChan:
			rf.me.savePersistState()
			rf.me.leader = unknownLeader
			rf.server.Stop()
			rf.waitExit <- struct{}{}
			return
		default:
			rf.mu.Lock()
			curState := rf.me.curState
			DPrintf(1, "before switch curState, ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
			rf.mu.Unlock()

			switch curState {
			case leaderState:
				rf.broadcastAppendEntries()
				select {
				case <-rf.voteGrantedChan:
				case <-rf.heartbeatChan:
				case <-time.After(time.Duration(rf.config.HeartBeatInterval) * time.Millisecond):
				}
				DPrintf(1, "leaderState, ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
			case followerState:
				select {
				case <-rf.heartbeatChan:
				case <-rf.voteGrantedChan:
				case <-time.After(time.Duration(rf.config.randElectionTimeout()) * time.Millisecond):
					rf.mu.Lock()
					rf.updateStateTo(candidateState)
					rf.me.savePersistState()
					DPrintf(1, "=============followerState to candidateState, ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
					rf.mu.Unlock()
				}
			case candidateState:
				go rf.broadcastRequestVotes()
				select {
				case <-rf.winVoteCampaign:
				case <-rf.heartbeatChan:
				case <-rf.voteGrantedChan:
				case <-time.After(time.Duration(rf.config.randElectionTimeout()) * time.Millisecond):
					rf.mu.Lock()
					rf.updateStateTo(candidateState)
					rf.me.savePersistState()
					DPrintf(1, "candidateState to candidateState, ID:%v, term:%v, curState:%v\n", rf.me.ID, rf.me.curTerm, rf.me.curState)
					rf.mu.Unlock()
				}
			}
		}
	}
}

const (
	ErrNotReadyCode  = 1
	ErrNotLeaderCode = 2
)

var (
	ErrNotReady  = errors.New("raft is not ready to provide service")
	ErrNotLeader = errors.New("current server is not leader")
)

func (rf *Raft) propose(entry *pb.Entry) (*pb.ProposeReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var reply = &pb.ProposeReply{
		Success: true,
	}

	if rf.me.leader == unknownLeader {
		reply.Success = false
		reply.ErrCode = ErrNotReadyCode
		reply.ErrMsg = ErrNotReady.Error()
		return reply, nil
	}

	if rf.me.leader == int64(rf.me.ID) {
		entry.Term = rf.me.curTerm
		entry.Index = rf.me.lastLogIndex() + 1
		rf.me.entries = append(rf.me.entries, entry)
		DPrintf(0, "propose: entries:%v\n", rf.me.entries)
		return reply, nil
	}

	reply.Success = false
	reply.ErrCode = ErrNotLeaderCode
	reply.ErrMsg = ErrNotLeader.Error()
	reply.Addr = rf.me.peers[rf.me.leader].Addr
	return reply, nil
}

// Propose proposes a log entry.
func (rf *Raft) Propose(ctx context.Context, args *pb.ProposeArgs) (*pb.ProposeReply, error) {
	var entry = &pb.Entry{
		Type: pb.EntryType_Normal,
		Data: args.Data,
	}
	return rf.propose(entry)
}

// ProposeConfChange proposes a config change log entry.
func (rf *Raft) ProposeConfChange(ctx context.Context, args *pb.ProposeArgs) (*pb.ProposeReply, error) {
	var entry = &pb.Entry{
		Type: pb.EntryType_Config,
		Data: args.Data,
	}
	return rf.propose(entry)
}

func Make(config *Config, peers []*Node, me *Node, storage Storage, applyMsg chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.heartbeatChan = make(chan struct{}, 16)
	rf.voteGrantedChan = make(chan struct{}, 16)
	rf.winVoteCampaign = make(chan struct{}, 16)
	rf.commitChan = make(chan struct{})
	rf.exitChan = make(chan struct{})
	rf.waitExit = make(chan struct{})
	if config == DefaultConfig {
		config.Peers = peers
	}
	rf.config = config
	rf.storage = storage

	rf.me.peers = peers
	rf.me.curState = followerState
	rf.me.leader = unknownLeader
	rf.me.curTerm = 0
	rf.me.votedFor = none
	rf.me.applyMsg = applyMsg
	rf.me.entries = append(rf.me.entries, &pb.Entry{Type: pb.EntryType_Normal, Term: 0, Index: 0})
	rf.me.persister = newPersister(rf.me.ID, rf.config.WorkDir)
	rf.me.readPersistState()

	go rf.registerRPCServer()
	go rf.run()
	go rf.commitLogs()

	return rf
}

func (rf *Raft) registerRPCServer() {
	lis, err := net.Listen("tcp", rf.me.Addr)
	if err != nil {
		panic("failed to listen:" + err.Error())
	}
	rf.server = grpc.NewServer()
	pb.RegisterRaftServer(rf.server, rf)
	reflection.Register(rf.server)
	if err := rf.server.Serve(lis); err != nil {
		log.Printf("failed to serve:" + err.Error())
	}
}
