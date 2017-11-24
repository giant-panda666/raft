package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
)

var ErrEof = errors.New("read current state machine eof")

// Storage is the interface used to create snapshot and recover state machine from snapshot.
// Clients must implement this interface or use the default storage.
type Storage interface {
	// Save saves current state machine and last index and term applied, and then returns last applied index and last applied term.
	Save() (uint64, uint64)

	// Apply use latest snapshot to recover state machine, and then returns last include index and last include term.
	Apply() (uint64, uint64)

	// Read returns latest snapshot and lastIncludeIndex, lastIncludeTerm.
	Read() ([]byte, uint64, uint64)

	// Write update snapshot received from another raft server to current server.
	Write([]byte, uint64, uint64)
}

// MemoryStorage is a example of implementation of Storage
type MemoryStorage struct {
	mu               sync.Mutex
	data             [][]byte
	lastAppliedIndex uint64
	lastAppliedTerm  uint64

	snapshot         []byte
	lastIncludeTerm  uint64
	lastIncludeIndex uint64

	applyMsg chan ApplyMsg
}

func NewMemoryStorage(applyMsg chan ApplyMsg) *MemoryStorage {
	return &MemoryStorage{
		applyMsg: applyMsg,
	}
}

func (s *MemoryStorage) Run() {
	for {
		select {
		case msg := <-s.applyMsg:
			s.mu.Lock()
			s.data = append(s.data, msg.Command)
			s.lastAppliedIndex = msg.Index
			s.lastAppliedTerm = msg.Term
			s.mu.Unlock()
		}
	}
}

func (s *MemoryStorage) Save() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	encoder.Encode(s.data)
	s.snapshot = buf.Bytes()
	s.lastIncludeIndex = s.lastAppliedIndex
	s.lastIncludeTerm = s.lastAppliedTerm

	return s.lastAppliedIndex, s.lastAppliedTerm
}

func (s *MemoryStorage) Apply() (uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := bytes.NewBuffer(s.snapshot)
	decoder := gob.NewDecoder(buf)
	decoder.Decode(&s.data)
	s.lastAppliedIndex = s.lastIncludeIndex
	s.lastAppliedTerm = s.lastIncludeTerm

	return s.lastAppliedIndex, s.lastAppliedTerm
}

func (s *MemoryStorage) Read() ([]byte, uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshot, s.lastIncludeIndex, s.lastIncludeTerm
}

func (s *MemoryStorage) Write(snapshot []byte, lastIncludeIndex, lastIncludeTerm uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = snapshot
	s.lastIncludeIndex = lastIncludeIndex
	s.lastIncludeTerm = lastIncludeTerm
	return
}
