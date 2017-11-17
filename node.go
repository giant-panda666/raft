package raft

import (
	pb "raft/raftpb"
)

// Node presents a raft server.
type Node struct {
	ID   uint64 // ID is a unique number to mark the raft server.
	Addr string // Addr is the address of raft server.

	// persistent state on all server nodes
	curTerm  uint64     // lastest term server has seen, initialized to 0 in first boot
	votedFor uint64     // candidateID that received vote in current term, null if none
	entries  []pb.Entry // log entries, first index is 1

	// volatile state
	curState    stateType // raft server node's state
	voteCounter uint64    // the number of votes received from other nodes.
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64
}

type stateType int

const (
	_ stateType = iota
	followerState
	leaderState
	candidateState
)

const none uint64 = 0 // none vote for somebody

func (n *Node) updateStateTo(st stateType, num int) {
	n.curState = st
	switch st {
	case followerState:
		n.votedFor = none
		n.voteCounter = 0
	case leaderState:
		n.nextIndex = make([]uint64, num)
		n.matchIndex = make([]uint64, num)
		for i := 0; i < num; i++ {
			n.nextIndex[i] = n.lastLogIndex() + 1
			n.matchIndex[i] = 0
		}
	case candidateState:
		n.curTerm++
		n.votedFor = n.ID
		n.voteCounter = 1
	}
}

func (n *Node) updateCurTerm(term uint64) {
	n.curTerm = term
}

func (n *Node) grantVote(candidateID uint64) {
	n.votedFor = candidateID
}

func (n *Node) lastLog() pb.Entry {
	return n.entries[len(n.entries)-1]
}

func (n *Node) lastLogTerm() uint64 {
	return n.lastLog().Term
}

func (n *Node) lastLogIndex() uint64 {
	return n.lastLog().Index
}
