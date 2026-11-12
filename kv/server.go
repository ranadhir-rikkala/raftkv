package kv

import (
	"fmt"
	"sync"
	"time"

	"github.com/ranadhir-rikkala/raftkv/raft"
	"github.com/ranadhir-rikkala/raftkv/transport"
)

// session is the duplicate-detection record for one client: the highest
// sequence number applied for it and the result that operation produced.
type session struct {
	Seq   int64
	Value string
	Err   Err
}

type result struct {
	value string
	err   Err
}

type waiter struct {
	clientID int64
	seq      int64
	ch       chan result
}

type Server struct {
	mu       sync.Mutex
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	done     chan struct{}
	killOnce sync.Once
	timeout  time.Duration

	data        map[string]string
	sessions    map[int64]session
	waiters     map[int]waiter
	lastApplied int
}

func NewServer(peers []transport.Endpoint, me int, persister raft.Persister, cfg raft.Config) (*Server, error) {
	kv := &Server{
		applyCh:  make(chan raft.ApplyMsg, 64),
		done:     make(chan struct{}),
		timeout:  time.Second,
		data:     make(map[string]string),
		sessions: make(map[int64]session),
		waiters:  make(map[int]waiter),
	}
	rf, err := raft.New(peers, me, persister, kv.applyCh, cfg)
	if err != nil {
		return nil, err
	}
	kv.rf = rf
	go kv.applyLoop()
	return kv, nil
}

func (kv *Server) Raft() *raft.Raft { return kv.rf }

func (kv *Server) Kill() {
	kv.killOnce.Do(func() {
		kv.rf.Kill()
		close(kv.done)
	})
}

func (kv *Server) Get(args *GetArgs, reply *GetReply) error {
	reply.Value, reply.Err = kv.submit(Op{Kind: OpGet, Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
	return nil
}

func (kv *Server) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	if args.Kind != OpPut && args.Kind != OpAppend {
		return fmt.Errorf("kv: invalid op kind %d", args.Kind)
	}
	_, reply.Err = kv.submit(Op{Kind: args.Kind, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq})
	return nil
}

// submit replicates op through Raft and waits until it is applied at the
// index Start returned. If a different op shows up at that index, or this
// server loses leadership, the client is told to try another server.
func (kv *Server) submit(op Op) (string, Err) {
	kv.mu.Lock()
	if s, ok := kv.sessions[op.ClientID]; ok && op.Seq <= s.Seq {
		kv.mu.Unlock()
		if op.Seq == s.Seq {
			return s.Value, s.Err
		}
		return "", OK
	}
	index, term, isLeader := kv.rf.Start(encodeOp(op))
	if !isLeader {
		kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	ch := make(chan result, 1)
	if old, ok := kv.waiters[index]; ok {
		old.ch <- result{err: ErrWrongLeader}
	}
	kv.waiters[index] = waiter{clientID: op.ClientID, seq: op.Seq, ch: ch}
	kv.mu.Unlock()

	timer := time.NewTimer(kv.timeout)
	defer timer.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case r := <-ch:
			return r.value, r.err
		case <-ticker.C:
			if t, leader := kv.rf.State(); t != term || !leader {
				kv.dropWaiter(index, ch)
				return "", ErrWrongLeader
			}
		case <-timer.C:
			kv.dropWaiter(index, ch)
			return "", ErrTimeout
		case <-kv.done:
			return "", ErrWrongLeader
		}
	}
}

func (kv *Server) dropWaiter(index int, ch chan result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if w, ok := kv.waiters[index]; ok && w.ch == ch {
		delete(kv.waiters, index)
	}
}

func (kv *Server) applyLoop() {
	for {
		select {
		case m := <-kv.applyCh:
			kv.apply(m)
		case <-kv.done:
			return
		}
	}
}

func (kv *Server) apply(m raft.ApplyMsg) {
	op, err := decodeOp(m.Command)
	if err != nil {
		panic(fmt.Sprintf("kv: index %d: %v", m.Index, err))
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if m.Index <= kv.lastApplied {
		return
	}
	kv.lastApplied = m.Index
	res := kv.execute(op)
	if w, ok := kv.waiters[m.Index]; ok {
		delete(kv.waiters, m.Index)
		if w.clientID == op.ClientID && w.seq == op.Seq {
			w.ch <- res
		} else {
			w.ch <- result{err: ErrWrongLeader}
		}
	}
}

// execute applies op to the state machine at most once per (ClientID, Seq).
// A retried op that was already applied returns the cached result instead of
// running again, which is what keeps a retried Append from appending twice.
func (kv *Server) execute(op Op) result {
	if s, ok := kv.sessions[op.ClientID]; ok && op.Seq <= s.Seq {
		if op.Seq == s.Seq {
			return result{value: s.Value, err: s.Err}
		}
		return result{err: OK}
	}
	r := result{err: OK}
	switch op.Kind {
	case OpGet:
		v, ok := kv.data[op.Key]
		if !ok {
			r.err = ErrNoKey
		}
		r.value = v
	case OpPut:
		kv.data[op.Key] = op.Value
	case OpAppend:
		kv.data[op.Key] += op.Value
	}
	kv.sessions[op.ClientID] = session{Seq: op.Seq, Value: r.value, Err: r.err}
	return r
}

func (kv *Server) Status(_ *StatusArgs, reply *StatusReply) error {
	reply.Term, _ = kv.rf.State()
	reply.Role = kv.rf.Role().String()
	kv.mu.Lock()
	reply.LastApplied = kv.lastApplied
	reply.Keys = len(kv.data)
	kv.mu.Unlock()
	return nil
}
