package raft

import "time"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

type Entry struct {
	Term    int
	Command []byte
}

// ApplyMsg is delivered on the apply channel once an entry is committed.
// Entries arrive in log order, exactly once per Raft incarnation.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte
}

type Config struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 250 * time.Millisecond,
		ElectionTimeoutMax: 500 * time.Millisecond,
		RPCTimeout:         500 * time.Millisecond,
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesReply carries the conflict hints from the "accelerated log
// backtracking" optimization (Raft paper section 5.3) so a leader can skip a
// whole term of mismatched entries per round trip instead of one entry.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	LogLen        int
}

type persistentState struct {
	Term     int
	VotedFor int
	Log      []Entry
}
