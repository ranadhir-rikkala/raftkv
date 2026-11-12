// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout),
// following the rules summarized in Figure 2 of the paper.
package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ranadhir-rikkala/raftkv/transport"
)

const tick = 10 * time.Millisecond

var errKilled = errors.New("raft: node is shut down")

type Raft struct {
	mu        sync.Mutex
	peers     []transport.Endpoint
	me        int
	persister Persister
	cfg       Config
	applyCh   chan<- ApplyMsg
	applyCond *sync.Cond
	done      chan struct{}
	dead      atomic.Bool
	killOnce  sync.Once
	rng       *rand.Rand

	role        Role
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionDeadline time.Time
	nextHeartbeat    time.Time
}

// New creates a Raft peer. peers[me] is never called. Committed entries are
// delivered on applyCh in index order.
func New(peers []transport.Endpoint, me int, persister Persister, applyCh chan<- ApplyMsg, cfg Config) (*Raft, error) {
	rf := &Raft{
		peers:      peers,
		me:         me,
		persister:  persister,
		cfg:        cfg,
		applyCh:    applyCh,
		done:       make(chan struct{}),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*7919)),
		votedFor:   -1,
		log:        []Entry{{Term: 0}},
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	if err := rf.restore(); err != nil {
		return nil, err
	}
	rf.resetElectionTimer()
	go rf.ticker()
	go rf.applier()
	return rf, nil
}

// Start proposes command for the log. It returns immediately; there is no
// guarantee the command will ever commit, since this peer may lose
// leadership. Callers learn the outcome from the apply channel.
func (rf *Raft) Start(command []byte) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index = rf.lastIndex()
	rf.matchIndex[rf.me] = index
	rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Role() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) Kill() {
	rf.killOnce.Do(func() {
		rf.dead.Store(true)
		close(rf.done)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	})
}

func (rf *Raft) killed() bool { return rf.dead.Load() }

func (rf *Raft) lastIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastTerm() int  { return rf.log[len(rf.log)-1].Term }

func (rf *Raft) persist() {
	var buf bytes.Buffer
	st := persistentState{Term: rf.currentTerm, VotedFor: rf.votedFor, Log: rf.log}
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		panic(fmt.Sprintf("raft: encode state: %v", err))
	}
	if err := rf.persister.Save(buf.Bytes()); err != nil {
		panic(fmt.Sprintf("raft: persist state: %v", err))
	}
}

func (rf *Raft) restore() error {
	data, err := rf.persister.Load()
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var st persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return fmt.Errorf("raft: decode state: %w", err)
	}
	if len(st.Log) == 0 {
		return fmt.Errorf("raft: persisted log is empty")
	}
	rf.currentTerm, rf.votedFor, rf.log = st.Term, st.VotedFor, st.Log
	return nil
}

func (rf *Raft) resetElectionTimer() {
	span := int64(rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin)
	d := rf.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(rf.rng.Int63n(span))
	}
	rf.electionDeadline = time.Now().Add(d)
}

func (rf *Raft) becomeFollower(term int) {
	changed := false
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		changed = true
	}
	rf.role = Follower
	if changed {
		rf.persist()
	}
}

func (rf *Raft) ticker() {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-rf.done:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		now := time.Now()
		if rf.role == Leader {
			if !now.Before(rf.nextHeartbeat) {
				rf.broadcastAppendEntries()
			}
		} else if now.After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) call(peer int, method string, args, reply any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.RPCTimeout)
	defer cancel()
	return rf.peers[peer].Call(ctx, method, args, reply) == nil
}

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1
	if len(rf.peers) == 1 {
		rf.becomeLeader()
		return
	}
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(p int) {
			reply := &RequestVoteReply{}
			if !rf.call(p, "Raft.RequestVote", args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	for p := range rf.peers {
		rf.nextIndex[p] = rf.lastIndex() + 1
		rf.matchIndex[p] = 0
	}
	rf.matchIndex[rf.me] = rf.lastIndex()
	rf.broadcastAppendEntries()
}

// RequestVote is the RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if rf.killed() {
		return errKilled
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
	return nil
}

// AppendEntries is the AppendEntries RPC handler, also used as heartbeat.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if rf.killed() {
		return errKilled
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.lastIndex() {
		reply.ConflictTerm = -1
		reply.LogLen = len(rf.log)
		return nil
	}
	if t := rf.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		reply.LogLen = len(rf.log)
		return nil
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndex() && rf.log[idx].Term == e.Term {
			continue
		}
		rf.log = append(rf.log[:idx], args.Entries[i:]...)
		rf.persist()
		break
	}

	if c := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)); c > rf.commitIndex {
		rf.commitIndex = c
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	return nil
}

func (rf *Raft) broadcastAppendEntries() {
	rf.nextHeartbeat = time.Now().Add(rf.cfg.HeartbeatInterval)
	for p := range rf.peers {
		if p != rf.me {
			go rf.replicate(p)
		}
	}
	if len(rf.peers) == 1 {
		rf.advanceCommit()
	}
}

func (rf *Raft) replicate(peer int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]
	prev := next - 1
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log[prev].Term,
		Entries:      append([]Entry(nil), rf.log[next:]...),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.call(peer, "Raft.AppendEntries", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.resetElectionTimer()
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Success {
		match := prev + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommit()
		return
	}
	if rf.nextIndex[peer] != next {
		return
	}
	rf.nextIndex[peer] = rf.backtrack(reply)
	go rf.replicate(peer)
}

func (rf *Raft) backtrack(reply *AppendEntriesReply) int {
	next := reply.LogLen
	if reply.ConflictTerm != -1 {
		next = reply.ConflictIndex
		for i := rf.lastIndex(); i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
			if rf.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	return max(1, min(next, rf.lastIndex()+1))
}

// advanceCommit applies the leader commit rule from Figure 2: commit index N
// if a majority has matchIndex >= N and log[N] is from the current term.
func (rf *Raft) advanceCommit() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break
		}
		count := 0
		for p := range rf.peers {
			if rf.matchIndex[p] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Term: rf.log[i].Term, Command: rf.log[i].Command})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, m := range msgs {
			select {
			case rf.applyCh <- m:
			case <-rf.done:
				return
			}
		}
	}
}
