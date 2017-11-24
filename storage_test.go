package raft

import "testing"

func TestMemoryStorage(t *testing.T) {
	var applyMsg = make(chan ApplyMsg)
	var s = NewMemoryStorage(applyMsg)
	go s.Run()

	applyMsg <- ApplyMsg{
		Index:   1,
		Term:    1,
		Command: []byte("hello"),
	}
	s.Save()

	applyMsg <- ApplyMsg{
		Index:   2,
		Term:    1,
		Command: []byte("world"),
	}
	s.Apply()
	if s.lastAppliedIndex != 1 || s.lastAppliedTerm != 1 || len(s.data) != 1 || string(s.data[0]) != "hello" {
		t.Errorf("MemoryStorage Save Apply failed...\n")
	}

	snapshot, index, term := s.Read()
	applyMsg <- ApplyMsg{
		Index:   2,
		Term:    1,
		Command: []byte("world"),
	}
	s.Save()
	s.Write(snapshot, index, term)
	s.Apply()
	if s.lastAppliedIndex != 1 || s.lastAppliedTerm != 1 || len(s.data) != 1 || string(s.data[0]) != "hello" {
		t.Errorf("MemoryStorage Save Apply failed...\n")
	}

	t.Logf("memory storage passed...")
}
