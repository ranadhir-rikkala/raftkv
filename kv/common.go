// Package kv is a linearizable key-value store replicated with Raft.
package kv

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	}
	return "?"
}

type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type StatusArgs struct {
	ClientID int64
}

type StatusReply struct {
	Term        int
	Role        string
	LastApplied int
	Keys        int
}
