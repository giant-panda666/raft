package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	pb "raft/raftpb"
)

const unknownLeader = -1

// Node presents a raft server.
type Node struct {
	ID   uint64 // ID is a unique number to mark the raft server.
	Addr string // Addr is the address of raft server.
	// peers are all the raft servers in the cluster
	peers []*Node

	leader int64 // current leader

	// persistent state on all server nodes
	curTerm  uint64      // lastest term server has seen, initialized to 0 in first boot
	votedFor uint64      // candidateID that received vote in current term, null if none
	entries  []*pb.Entry // log entries, first index is 1

	// volatile state
	curState    stateType // raft server node's state
	voteCounter uint64    // the number of votes received from other nodes.
	commitIndex uint64
	lastApplied uint64
	nextIndex   []uint64
	matchIndex  []uint64

	applyMsg chan ApplyMsg

	// persister
	persister *persister
}

type stateType int

const (
	_ stateType = iota
	followerState
	leaderState
	candidateState
)

func (n *Node) readPersistState() {
	data, err := n.persister.readPersistState()
	if err == ErrNoPersisterFile {
		return
	}

	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	decoder.Decode(&n.curTerm)
	decoder.Decode(&n.votedFor)
	decoder.Decode(&n.entries)
}

func (n *Node) savePersistState() {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	encoder.Encode(n.curTerm)
	encoder.Encode(n.votedFor)
	encoder.Encode(n.entries)
	err := n.persister.savePersistState(buf.Bytes())
	if err != nil {
		log.Printf("node:%v savePersistState curTerm:%v, votedFor:%v\nlog entries:%v\nerror:%v\n", n.ID, n.curTerm, n.votedFor, n.entries, err)
	}
}

const none uint64 = 0 // none vote for somebody

func (n *Node) updateStateTo(st stateType) {
	n.curState = st
	switch st {
	case followerState:
		n.votedFor = none
		n.voteCounter = 0
	case leaderState:
		n.leader = int64(n.ID)
		n.nextIndex = make([]uint64, len(n.peers))
		n.matchIndex = make([]uint64, len(n.peers))
		for i := 0; i < len(n.peers); i++ {
			n.nextIndex[i] = n.lastLogIndex() + 1
			n.matchIndex[i] = 0
		}
	case candidateState:
		n.leader = unknownLeader
		n.curTerm++
		n.votedFor = n.ID
		n.voteCounter = 1
	}
}

func (n *Node) updateCommitIndex() bool {
	baseIndex := n.baseIndex()
	for i := n.lastLogIndex(); i > n.commitIndex; i-- {
		if n.entries[i-baseIndex].Term != n.curTerm {
			break
		}
		count := 0
		for j := range n.peers {
			if n.matchIndex[j] >= i {
				count++
			}
		}

		if 2*count > len(n.peers) {
			n.commitIndex = i
			return true
		}
	}
	return false
}

func (n *Node) commitLogs() {
	for n.lastApplied < n.commitIndex {
		l, ok := n.getCommitLog(n.lastApplied + 1)
		if !ok {
			return
		}
		// todo chang config(membership changes)
		// this won't be passed to applyMsg
		if l.Type == pb.EntryType_Config {
			return
		}
		n.lastApplied++
		msg := ApplyMsg{Index: n.lastApplied, Term: l.Term, Command: l.Data}
		n.applyMsg <- msg
	}
}

func (n *Node) updateCurTerm(term uint64) {
	n.curTerm = term
}

func (n *Node) grantVote(candidateID uint64) {
	n.votedFor = candidateID
}

func (n *Node) getCommitLog(index uint64) (*pb.Entry, bool) {
	if index < n.baseIndex() || index > uint64(len(n.entries)) {
		return &pb.Entry{}, false
	}
	return n.entries[index-n.baseIndex()], true
}

// lastLog returns last log in the entries
func (n *Node) lastLog() *pb.Entry {
	return n.entries[len(n.entries)-1]
}

// lastLogTerm returns  last log's term in the entries
func (n *Node) lastLogTerm() uint64 {
	return n.lastLog().Term
}

// lastLogIndex returns last log's index in the entries
func (n *Node) lastLogIndex() uint64 {
	return n.lastLog().Index
}

// baseIndex returns the max index that has been snapshot or the first entry's index
func (n *Node) baseIndex() uint64 {
	return n.entries[0].Index
}

func (n *Node) prevLog(peer uint64) *pb.Entry {
	index := n.nextIndex[peer] - 1
	baseIndex := n.baseIndex()
	DPrintf(0, "peer:%v, index:%v, baseIndex:%v\n", peer, index, baseIndex)
	if index < baseIndex {
		return &pb.Entry{
			Type:  pb.EntryType_None,
			Term:  0,
			Index: 0,
		}
	} else if index <= n.lastLogIndex() {
		return n.entries[index-baseIndex]
	} else { // entry in nextIndex[peer] has been deleted, just sends the last entry
		return n.entries[n.lastLogIndex()-baseIndex]
	}
}
