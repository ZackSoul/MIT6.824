package kvraft

import "time"

type OperationOp uint8

const ExecuteTimeout = 500 * time.Millisecond

const (
	OpPut OperationOp = iota
	OpAppend
	OpGet
)

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

type CommandReply struct {
	Err   Err
	Value string
}
type Command struct {
	*CommandArgs
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
