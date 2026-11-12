package kv

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ranadhir-rikkala/raftkv/raft"
	"github.com/ranadhir-rikkala/raftkv/transport"
	"github.com/ranadhir-rikkala/raftkv/transport/memnet"
)

type cluster struct {
	t          *testing.T
	n          int
	net        *memnet.Network
	cfg        raft.Config
	rngMu      sync.Mutex
	rng        *rand.Rand
	mu         sync.Mutex
	servers    []*Server
	persisters []*raft.MemoryPersister
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

func newCluster(t *testing.T, n int, reliable bool) *cluster {
	seed := testSeed(t)
	c := &cluster{
		t:          t,
		n:          n,
		net:        memnet.New(seed),
		cfg:        raft.DefaultConfig(),
		rng:        rand.New(rand.NewSource(seed)),
		servers:    make([]*Server, n),
		persisters: make([]*raft.MemoryPersister, n),
	}
	c.net.SetReliable(reliable)
	for i := range n {
		c.start(i)
	}
	begin := time.Now()
	t.Cleanup(func() {
		for i := range n {
			c.crash(i)
		}
		c.net.Close()
		t.Logf("elapsed=%v rpcs=%d", time.Since(begin).Round(time.Millisecond), c.net.RPCCount())
	})
	return c
}

func (c *cluster) start(i int) {
	c.crash(i)
	srv := memnet.NewServer()
	c.net.AddServer(i, srv)
	ends := make([]transport.Endpoint, c.n)
	for j := range c.n {
		ends[j] = c.net.Endpoint(i, j)
	}
	c.mu.Lock()
	if c.persisters[i] == nil {
		c.persisters[i] = raft.NewMemoryPersister()
	}
	p := c.persisters[i]
	c.mu.Unlock()

	kv, err := NewServer(ends, i, p, c.cfg)
	if err != nil {
		c.t.Fatalf("start %d: %v", i, err)
	}
	if err := srv.Register("Raft", kv.Raft()); err != nil {
		c.t.Fatal(err)
	}
	if err := srv.Register("KV", kv); err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.servers[i] = kv
	c.mu.Unlock()
}

func (c *cluster) crash(i int) {
	c.net.Crash(i)
	c.mu.Lock()
	kv := c.servers[i]
	c.servers[i] = nil
	if c.persisters[i] != nil {
		c.persisters[i] = c.persisters[i].Copy()
	}
	c.mu.Unlock()
	if kv != nil {
		kv.Kill()
	}
}

func (c *cluster) clerk() *Clerk {
	return c.clerkFor(seq(c.n)...)
}

func (c *cluster) clerkFor(servers ...int) *Clerk {
	ends := make([]transport.Endpoint, len(servers))
	for i, s := range servers {
		ends[i] = c.net.Endpoint(memnet.Client, s)
	}
	return NewClerk(ends)
}

func (c *cluster) leader() (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, kv := range c.servers {
		if kv == nil {
			continue
		}
		if _, isLeader := kv.Raft().State(); isLeader && c.net.IsConnected(i) {
			return i, true
		}
	}
	return -1, false
}

func (c *cluster) waitLeader() int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok := c.leader(); ok {
			return l
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1
}

func (c *cluster) intn(n int) int {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Intn(n)
}

func (c *cluster) perm(n int) []int {
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	return c.rng.Perm(n)
}

func seq(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}
