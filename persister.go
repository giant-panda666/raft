package raft

import (
	"errors"
	"io/ioutil"
	"os"
	"strconv"
)

// persister persists raft states which must be persisted. All raft states which must be persisted are curTerm, votedFor, entries.
type persister struct {
	fileName string // persist file name
}

var ErrNoPersisterFile = errors.New("persister file not exists")

func (p *persister) readPersistState() ([]byte, error) {
	var res []byte
	var err error
	res, err = ioutil.ReadFile(p.fileName)
	if err != nil && os.IsNotExist(err) {
		return res, ErrNoPersisterFile
	}
	return res, err
}

func (p *persister) savePersistState(data []byte) error {
	f, err := os.OpenFile(p.fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	err = f.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (p *persister) removePersistFile() {
	os.Remove(p.fileName)
}

func newPersister(id uint64, dir string) *persister {
	if dir != "" && string(dir[len(dir)-1]) != "/" {
		dir = dir + "/"
	}
	return &persister{
		fileName: dir + strconv.FormatUint(id, 10) + ".raft",
	}
}
