package raft

import (
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
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Raft struct {
	// Lock to protect shared access to this peer's state
	mu sync.Mutex
	// peers are all the raft servers in the cluster
	peers []*Node
	// current raft server
	me *Node
	// config
	config *Config

	heartbeatChan   chan struct{}
	voteGrantedChan chan struct{}
	winVoteCampaign chan struct{}
}

func (rf *Raft) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	return nil, nil
}

func (rf *Raft) RequestVotes(ctx context.Context, args *pb.RequestVotesArgs) (*pb.RequestVotesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var err error
	var reply = &pb.RequestVotesReply{
		Term:        rf.me.curTerm,
		VoteGranted: false,
	}

	if rf.me.curTerm > args.Term || rf.me.votedFor == args.CandidateID {
		return reply, err
	} else if rf.me.curTerm < args.Term {
		rf.me.updateCurTerm(args.Term)
		rf.updateStateTo(followerState)
		reply.Term = args.Term
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
		rf.voteGrantedChan <- struct{}{}
	}

	return reply, err
}

func (rf *Raft) updateStateTo(st stateType) {
	rf.me.updateStateTo(st, len(rf.peers))
}

func (rf *Raft) sendRequestVote(peer uint64, args *pb.RequestVotesArgs) (reply *pb.RequestVotesReply, err error) {
	conn, err := grpc.Dial(rf.peers[peer].Addr, grpc.WithInsecure())
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
	} else if reply.VoteGranted {
		rf.me.voteCounter++
		if rf.me.voteCounter > uint64(len(rf.peers)>>1) || rf.me.curState == candidateState {
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
	rf.mu.Unlock()
	for i := range rf.peers {
		if uint64(i) != rf.me.ID {
			go rf.doRequestVote(uint64(i), args)
		}
	}
}

func (rf *Raft) run() {
	for {
		// for-testing
		stopMu.Lock()
		res := stopRaftServer
		stopMu.Unlock()
		if res {
			return
		}

		rf.mu.Lock()
		curState := rf.me.curState
		rf.mu.Unlock()

		switch curState {
		case leaderState:
			// rf.broadcastAppendEntries()
			time.Sleep(time.Duration(rf.config.HeartBeatInterval) * time.Millisecond)
		case followerState:
			select {
			case <-rf.heartbeatChan:
			case <-rf.voteGrantedChan:
			case <-time.After(time.Duration(rf.config.randElectionTimeout()) * time.Millisecond):
				rf.mu.Lock()
				rf.updateStateTo(candidateState)
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
				rf.mu.Unlock()
			}
		}
	}
}

func Make(config *Config, peers []*Node, me *Node, applyMsg chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.heartbeatChan = make(chan struct{})
	rf.voteGrantedChan = make(chan struct{})
	rf.winVoteCampaign = make(chan struct{})
	rf.config = config

	rf.me.curState = followerState
	rf.me.curTerm = 0
	rf.me.votedFor = none
	rf.me.entries = append(rf.me.entries, pb.Entry{Term: 0, Index: 0})

	go registerRPCServer(rf)
	go rf.run()

	return rf
}

func registerRPCServer(rf *Raft) {
	lis, err := net.Listen("tcp", rf.me.Addr)
	if err != nil {
		panic("failed to listen:" + err.Error())
	}
	s := grpc.NewServer()
	pb.RegisterRaftServer(s, rf)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("failed to serve:" + err.Error())
	}
}
