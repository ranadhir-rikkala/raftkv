package raft

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ranadhir-rikkala/raftkv/transport"
	"github.com/ranadhir-rikkala/raftkv/transport/memnet"
)

const electionTimeout = time.Second

type applyLog struct {
	entries map[int]string
}

type harness struct {
	t          *testing.T
	n          int
	net        *memnet.Network
	cfg        Config
	rng        *rand.Rand
	mu         sync.Mutex
	rafts      []*Raft
	persisters []*MemoryPersister
	logs       []*applyLog
	stops      []chan struct{}
	connected  []bool
	applyErr   string
	maxIndex   int
	begin      time.Time
}

func testSeed(t *testing.T) int64 {
	seed := time.Now().UnixNano()
	if s := os.Getenv("RAFTKV_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("bad RAFTKV_SEED: %v", err)
		}
		seed = v
	}
	t.Logf("RAFTKV_SEED=%d", seed)
	return seed
}

func newHarness(t *testing.T, n int, reliable bool) *harness {
	seed := testSeed(t)
	h := &harness{
		t:          t,
		n:          n,
		net:        memnet.New(seed),
		cfg:        DefaultConfig(),
		rng:        rand.New(rand.NewSource(seed)),
		rafts:      make([]*Raft, n),
		persisters: make([]*MemoryPersister, n),
		logs:       make([]*applyLog, n),
		stops:      make([]chan struct{}, n),
		connected:  make([]bool, n),
		begin:      time.Now(),
	}
	h.net.SetReliable(reliable)
	for i := range n {
		h.start(i)
	}
	for i := range n {
		h.connect(i)
	}
	t.Cleanup(h.cleanup)
	return h
}

func (h *harness) cleanup() {
	for i := range h.n {
		h.crash(i)
	}
	h.net.Close()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.applyErr != "" {
		h.t.Error(h.applyErr)
	}
	h.t.Logf("elapsed=%v rpcs=%d max_index=%d", time.Since(h.begin).Round(time.Millisecond), h.net.RPCCount(), h.maxIndex)
}

func (h *harness) crash(i int) {
	h.net.Crash(i)
	h.mu.Lock()
	rf := h.rafts[i]
	h.rafts[i] = nil
	if h.persisters[i] != nil {
		h.persisters[i] = h.persisters[i].Copy()
	}
	stop := h.stops[i]
	h.stops[i] = nil
	h.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
	if stop != nil {
		close(stop)
	}
}

func (h *harness) start(i int) {
	h.crash(i)

	srv := memnet.NewServer()
	h.net.AddServer(i, srv)
	ends := make([]transport.Endpoint, h.n)
	for j := range h.n {
		ends[j] = h.net.Endpoint(i, j)
	}

	h.mu.Lock()
	if h.persisters[i] == nil {
		h.persisters[i] = NewMemoryPersister()
	}
	p := h.persisters[i]
	lg := &applyLog{entries: make(map[int]string)}
	h.logs[i] = lg
	stop := make(chan struct{})
	h.stops[i] = stop
	h.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	go h.consume(i, lg, applyCh, stop)

	rf, err := New(ends, i, p, applyCh, h.cfg)
	if err != nil {
		h.t.Fatalf("start %d: %v", i, err)
	}
	if err := srv.Register("Raft", rf); err != nil {
		h.t.Fatal(err)
	}
	h.mu.Lock()
	h.rafts[i] = rf
	h.mu.Unlock()
}

func (h *harness) consume(i int, lg *applyLog, ch <-chan ApplyMsg, stop <-chan struct{}) {
	for {
		select {
		case m := <-ch:
			h.record(i, lg, m)
		case <-stop:
			return
		}
	}
}

func (h *harness) record(i int, lg *applyLog, m ApplyMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := string(m.Command)
	for j, other := range h.logs {
		if other == nil {
			continue
		}
		if old, ok := other.entries[m.Index]; ok && old != v {
			h.applyErr = fmt.Sprintf("commit index=%d server=%d %q != server=%d %q", m.Index, i, v, j, old)
		}
	}
	if _, ok := lg.entries[m.Index-1]; m.Index > 1 && !ok {
		h.applyErr = fmt.Sprintf("server %d applied index %d out of order", i, m.Index)
	}
	lg.entries[m.Index] = v
	h.maxIndex = max(h.maxIndex, m.Index)
}

func (h *harness) connect(i int) {
	h.net.Connect(i)
	h.mu.Lock()
	h.connected[i] = true
	h.mu.Unlock()
}

func (h *harness) disconnect(i int) {
	h.net.Disconnect(i)
	h.mu.Lock()
	h.connected[i] = false
	h.mu.Unlock()
}

func (h *harness) isConnected(i int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.connected[i]
}

func (h *harness) raft(i int) *Raft {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rafts[i]
}

func (h *harness) failIfApplyErr() {
	h.mu.Lock()
	err := h.applyErr
	h.mu.Unlock()
	if err != "" {
		h.t.Fatal(err)
	}
}

func (h *harness) checkOneLeader() int {
	h.t.Helper()
	for range 10 {
		time.Sleep(time.Duration(450+h.rng.Intn(100)) * time.Millisecond)
		leaders := make(map[int][]int)
		for i := range h.n {
			rf := h.raft(i)
			if rf == nil || !h.isConnected(i) {
				continue
			}
			if term, isLeader := rf.State(); isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		last := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				h.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			last = max(last, term)
		}
		if last != -1 {
			return leaders[last][0]
		}
	}
	h.t.Fatal("expected one leader, got none")
	return -1
}

func (h *harness) checkTerms() int {
	h.t.Helper()
	term := -1
	for i := range h.n {
		rf := h.raft(i)
		if rf == nil || !h.isConnected(i) {
			continue
		}
		t, _ := rf.State()
		if term == -1 {
			term = t
		} else if t != term {
			h.t.Fatalf("servers disagree on term: %d vs %d", term, t)
		}
	}
	return term
}

func (h *harness) checkNoLeader() {
	h.t.Helper()
	for i := range h.n {
		rf := h.raft(i)
		if rf == nil || !h.isConnected(i) {
			continue
		}
		if _, isLeader := rf.State(); isLeader {
			h.t.Fatalf("expected no leader among connected servers, but %d claims to be leader", i)
		}
	}
}

func (h *harness) nCommitted(index int) (int, string) {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.applyErr != "" {
		h.t.Fatal(h.applyErr)
	}
	count, cmd := 0, ""
	for i := range h.n {
		if h.logs[i] == nil {
			continue
		}
		v, ok := h.logs[i].entries[index]
		if !ok {
			continue
		}
		if count > 0 && v != cmd {
			h.t.Fatalf("committed values differ at index %d: %q vs %q", index, cmd, v)
		}
		count++
		cmd = v
	}
	return count, cmd
}

func (h *harness) waitCommitted(index, n, startTerm int) string {
	h.t.Helper()
	backoff := 10 * time.Millisecond
	for range 30 {
		if c, _ := h.nCommitted(index); c >= n {
			break
		}
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
		if startTerm > -1 {
			for i := range h.n {
				if rf := h.raft(i); rf != nil {
					if t, _ := rf.State(); t > startTerm {
						return ""
					}
				}
			}
		}
	}
	c, cmd := h.nCommitted(index)
	if c < n {
		h.t.Fatalf("only %d servers committed index %d, want %d", c, index, n)
	}
	return cmd
}

// one submits cmd to whichever server claims to be leader and waits for it
// to be committed by at least expected servers. With retry it resubmits if
// the first leader fails before committing.
func (h *harness) one(cmd string, expected int, retry bool) int {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	next := 0
	for time.Now().Before(deadline) {
		index := -1
		for range h.n {
			s := next
			next = (next + 1) % h.n
			rf := h.raft(s)
			if rf == nil || !h.isConnected(s) {
				continue
			}
			if i, _, ok := rf.Start([]byte(cmd)); ok {
				index = i
				break
			}
		}
		if index != -1 {
			until := time.Now().Add(2 * time.Second)
			for time.Now().Before(until) {
				if c, v := h.nCommitted(index); c > 0 && c >= expected && v == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				h.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	h.t.Fatalf("one(%q) failed to reach agreement within 10s", cmd)
	return -1
}
