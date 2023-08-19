package shardctrler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	stateMachine   ConfigStateMachine
	lastOperations map[int64]OperationContext
	notifyChans    map[int]chan *CommandResponse
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Command(request *CommandRequest, response *CommandResponse) {
	sc.mu.Lock()
	if request.Op != OpQuery && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := sc.lastOperations[request.ClientId].LastResponse
		response.Config, response.Err = lastResponse.Config, lastResponse.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		response.Config, response.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	go func() {
		sc.mu.Lock()
		sc.removeOutDatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, requestId int64) bool {
	OperationContext, ok := sc.lastOperations[clientId]
	return ok && requestId <= OperationContext.MaxAppliedCommandId
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeOutDatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandResponse {
	var config Config
	var err Err
	switch command.Op {
	case OpJoin:
		err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(command.Num)
	}
	return &CommandResponse{err, config}
}

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				var response *CommandResponse
				command := message.Command.(Command)
				sc.mu.Lock()
				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					response = sc.lastOperations[command.ClientId].LastResponse
				} else {
					response = sc.applyLogToStateMachine(command)
					if command.Op != OpQuery {
						sc.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				sc.mu.Unlock()

			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	labgob.Register(Command{})
	sc.me = me
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.dead = 0
	sc.stateMachine = NewMemoryConfigStateMachine()
	sc.lastOperations = make(map[int64]OperationContext)
	sc.notifyChans = make(map[int]chan *CommandResponse)

	// Your code here.
	go sc.applier()

	return sc
}
