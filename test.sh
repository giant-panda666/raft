#!/bin/bash

go test -v -run=TestMemoryStorage
go test -v -run=TestInitialElection
go test -v -run=TestPersistRaftState
go test -v -run=TestSnapshot
go test -v -run=TestLogReplication
