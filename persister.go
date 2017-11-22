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

// for-test
var workDir = "/tmp/"

// SetPersisterFileDir set the directory to save persister file. If dir is "", use the default directory /tmp.
func SetPersisterFileDir(dir string) {
	if dir == "" {
		return
	}
	if string(dir[len(dir)-1]) == "/" {
		workDir = dir
	} else {
		workDir = dir + "/"
	}
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

func newPersister(id uint64) *persister {
	return &persister{
		fileName: workDir + strconv.FormatUint(id, 10) + ".raft",
	}
}
