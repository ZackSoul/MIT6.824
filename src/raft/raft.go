package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

type VoteState int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota
	Killed
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChan chan ApplyMsg

	state State

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	replicatorCond []*sync.Cond
	applyCond      *sync.Cond
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) matchLog(prevLogIndex int, prevLogTerm int) bool {
	return prevLogIndex <= rf.getLastLog().Index && rf.logs[prevLogIndex-rf.getFirstLog().Index].Term == prevLogTerm
}

func (rf *Raft) UpdateFollowerCommitIndex(LeaderCommit int) {
	newCommitIndex := Min(LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex, newCommitIndex, LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(ElectionTimeout())
	// reply.Success = true
	// reply.Term = rf.currentTerm
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success, reply.Term = false, rf.currentTerm
		lastLogIndex := rf.getLastLog().Index
		if args.PrevLogIndex > lastLogIndex {
			reply.ConflictTerm, reply.ConflictIndex = -1, len(rf.logs)
		} else {
			firstLogIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstLogIndex].Term
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
		return
	}

	lastLogIndex := rf.getLastLog().Index
	for i, entry := range args.Entries {
		index := i + args.PrevLogIndex + 1
		if index > lastLogIndex || rf.logs[index].Term != entry.Term {
			rf.logs = rf.logs[:index]
			copyEntries := make([]LogEntry, 0)
			copyEntries = append(copyEntries, args.Entries[i:]...)
			rf.logs = append(rf.logs, copyEntries...)
			break
		}
	}
	rf.UpdateFollowerCommitIndex(args.LeaderCommit)
	reply.Success, reply.Term = true, rf.currentTerm

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteArgs %v and reply requestVoteReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(ElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return ok

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	// if reply.Success {
	// 	return true
	// } else {
	// 	if reply.Term > rf.currentTerm {
	// 		rf.currentTerm = reply.Term
	// 		rf.votedFor = -1
	// 		rf.ChangeState(Follower)
	// 		rf.electionTimer.Reset(ElectionTimeout())
	// 	}
	// 	return false
	// }
}

func (rf *Raft) upDateLeaderCommitIndex() {
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sortedMatchIndex[rf.me] = rf.getLastLog().Index
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	if rf.state == Leader && newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
		DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	} else {
		DPrintf("{Node %d} can not advance commitIndex from %d to newCommitIndex %b ecause the term of newCommitIndexTerm %d is not equal to currentTerm %d", rf.me, rf.commitIndex, newCommitIndex, rf.logs[newCommitIndex].Term, rf.currentTerm)
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state == Leader && rf.currentTerm == args.Term {
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.upDateLeaderCommitIndex()
		} else {
			if reply.Term > rf.currentTerm {
				rf.ChangeState(Follower)
				rf.currentTerm, rf.votedFor = reply.Term, -1
			} else if reply.Term == rf.currentTerm {
				rf.nextIndex[server] = reply.ConflictIndex
				if reply.ConflictTerm != -1 {
					firstLogIndex := rf.getFirstLog().Index
					for i := args.PrevLogIndex; i >= firstLogIndex; i-- {
						if rf.logs[i-firstLogIndex].Term == reply.ConflictTerm {
							rf.nextIndex[server] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), reply, args)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	newLog := rf.appendNewLog(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)

	rf.BroadcastHeartbeat(false)

	return newLog.Index, newLog.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(ElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(HeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstlogIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		// DPrintf("{Node %v} lastApplied %v commitIndex %v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.logs[rf.lastApplied+1-firstlogIndex:rf.commitIndex+1-firstlogIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		// DPrintf("{Node %v} lastApplied %v commitIndex %v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartElection() {
	requestVoteArgs := rf.GenRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, requestVoteArgs)
	grandtedVotes := 1
	rf.votedFor = rf.me
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			requestVoteReply := newRequestVoteReply()
			if rf.sendRequestVote(peer, &requestVoteArgs, &requestVoteReply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, requestVoteReply, peer, requestVoteArgs, rf.currentTerm)
				if rf.currentTerm == requestVoteArgs.Term && rf.state == Candidate {
					if requestVoteReply.VoteGranted {
						grandtedVotes += 1
						if grandtedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
							rf.heartbeatTimer.Reset(HeartbeatTimeout())
						} else if requestVoteReply.Term > rf.currentTerm {
							DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, requestVoteReply.Term, rf.currentTerm)
							rf.ChangeState(Follower)
							rf.currentTerm, rf.votedFor = requestVoteReply.Term, -1
						}
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) replicateEntry(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	appendEntriesArgs := rf.genAppendEntries(prevLogIndex)
	appendEntriesReply := rf.newAppendEntriesReply()
	rf.mu.RUnlock()
	if rf.SendAppendEntries(peer, &appendEntriesArgs, &appendEntriesReply) {
		rf.mu.Lock()
		rf.handleAppendEntriesReply(peer, &appendEntriesArgs, &appendEntriesReply)
		rf.mu.Unlock()
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			go rf.replicateEntry(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(server int) {
	rf.replicatorCond[server].L.Lock()
	defer rf.replicatorCond[server].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(server) {
			rf.replicatorCond[server].Wait()
		}
		rf.replicateEntry(server)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyChan:      applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		electionTimer:  time.NewTimer(ElectionTimeout()),
		heartbeatTimer: time.NewTimer(HeartbeatTimeout()),
	}
	// fmt.Println("初始化完成")
	// Your initialization code here (2A, 2B, 2C).
	// fmt.Println("启动线程1")
	// fmt.Println("启动线程2")
	rf.readPersist(persister.ReadRaftState())
	lastLog := rf.getLastLog()
	rf.applyCond = sync.NewCond(&rf.mu)

	for server := 0; server < len(rf.peers); server++ {
		// fmt.Println("启动线程3")
		rf.matchIndex[server], rf.nextIndex[server] = 0, lastLog.Index+1
		if server != rf.me {
			rf.replicatorCond[server] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(server)
		}
	}
	// fmt.Println("启动线程4")

	// initialize from state persisted before a crash

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
