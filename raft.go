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

type ApplyMsg struct {
	Index       uint64
	Command     []byte
	UseSnapshot bool
	Snapshot    []byte
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
}

func (rf *Raft) RequestVotes(ctx context.Context, args *pb.RequestVotesArgs) (*pb.RequestVotesReply, error) {
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
			DPrintf(0, "////////////////////leader:%v, vote:%v, uint64(len(rf.me.peers)/2):%v\n", rf.me.ID, rf.me.voteCounter, uint64(len(rf.me.peers)/2))
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
	}
	reply.Success = true
	if args.LeaderCommit > rf.me.commitIndex {
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
	if rf.me.updateCommitIndex() {
		rf.commitChan <- struct{}{}
	}

	args := pb.AppendEntriesArgs{
		Term:         rf.me.curTerm,
		LeaderID:     rf.me.ID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.me.commitIndex,
	}

	for i := range rf.me.peers {
		if uint64(i) == rf.me.ID {
			continue
		}
		prevLogEntry := rf.me.prevLog(uint64(i))
		//		if prevLogEntry.Type == pb.EntryType_Normal && prevLogEntry.Term == 0 {
		//			// sends heartbeats
		//			go rf.doAppendEntry(uint64(i), &args)
		//			continue
		//		}
		if prevLogEntry.Type == pb.EntryType_None {
			// todo needs to send snapshot
			continue
		}
		// sends current entry to the end
		baseIndex := rf.me.baseIndex()
		args.PrevLogIndex = prevLogEntry.Index
		args.PrevLogTerm = prevLogEntry.Term
		args.Entries = make([]*pb.Entry, len(rf.me.entries[args.PrevLogIndex+1-baseIndex:]))
		copy(args.Entries, rf.me.entries[args.PrevLogIndex+1-baseIndex:])
		DPrintf(0, "baseIndex:%v, prevLogIndex:%v, PrevLogTerm:%v, rf.me.entries:%v, args.Entries:%v\n", baseIndex, args.PrevLogIndex, args.PrevLogTerm, rf.me.entries, args.Entries)
		go rf.doAppendEntry(uint64(i), &args)
	}
}

func (rf *Raft) Kill() {
	if rf.exited {
		return
	}
	rf.server.Stop()
	rf.exitChan <- struct{}{}
	rf.exited = true
}

func (rf *Raft) commitLogs() {
	for {
		select {
		case <-rf.commitChan:
			rf.mu.Lock()
			rf.me.commitLogs()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.exitChan:
			// todo some persist
			rf.me.savePersistState()
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

func Make(config *Config, peers []*Node, me *Node, applyMsg chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.heartbeatChan = make(chan struct{}, 16)
	rf.voteGrantedChan = make(chan struct{}, 16)
	rf.winVoteCampaign = make(chan struct{}, 16)
	rf.commitChan = make(chan struct{})
	rf.exitChan = make(chan struct{})
	rf.config = config

	rf.me.peers = peers
	rf.me.curState = followerState
	rf.me.leader = unknownLeader
	rf.me.curTerm = 0
	rf.me.votedFor = none
	rf.me.applyMsg = applyMsg
	rf.me.entries = append(rf.me.entries, &pb.Entry{Type: pb.EntryType_Normal, Term: 0, Index: 0})
	rf.me.persister = newPersister(rf.me.ID)
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
