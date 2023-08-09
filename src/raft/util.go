package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

func (rf *Raft) ChangeState(state State) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %v} changes state from %v to %v in term %v", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(ElectionTimeout())
	case Candidate:
	case Leader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatTimeout())
	}
}

const (
	HeartbeatTime = 125
	ElectionTime  = 1000
)

func HeartbeatTimeout() time.Duration {
	// return 120 * time.Millisecond
	return time.Duration(HeartbeatTime) * time.Millisecond
}

func ElectionTimeout() time.Duration {
	return time.Duration(ElectionTime+globalRand.Intn(ElectionTime)) * time.Millisecond
	// 	return time.Duration(150+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) GenRequestVoteArgs() RequestVoteArgs {
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  0,
	}
	if len(rf.logs) > 0 {
		voteArgs.LastLogTerm = rf.logs[voteArgs.LastLogIndex].Term
	}
	return voteArgs
}

func newRequestVoteReply() RequestVoteReply {
	return RequestVoteReply{}
}

func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	lastLog := rf.getLastLog()
	return lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index)
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) appendNewLog(command interface{}) LogEntry {
	newLog := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.getLastLog().Index + 1,
		Command: command,
	}
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	return newLog
}

func (rf *Raft) genAppendEntries(prevLogIndex int) AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	entries = rf.logs[prevLogIndex-firstLogIndex+1:]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) newAppendEntriesReply() AppendEntriesReply {
	return AppendEntriesReply{}
}

func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func (rf *Raft) newInstallSnapshotReply() InstallSnapshotReply {
	return InstallSnapshotReply{}
}

func shrinkEntriesArray(entries []LogEntry) []LogEntry {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}
