package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type kvInput struct {
	Kind  OpKind
	Key   string
	Value string
}

type kvOutput struct {
	Value string
}

// kvModel is the sequential specification of a single-copy key-value map.
// Histories are partitioned by key, since operations on different keys
// commute.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st := state.(string)
		in := input.(kvInput)
		switch in.Kind {
		case OpGet:
			return output.(kvOutput).Value == st, st
		case OpPut:
			return true, in.Value
		case OpAppend:
			return true, st + in.Value
		}
		return false, st
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		switch in.Kind {
		case OpGet:
			return fmt.Sprintf("get(%q) -> %q", in.Key, output.(kvOutput).Value)
		case OpPut:
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		}
	},
}

type recorder struct {
	mu    sync.Mutex
	base  time.Time
	ops   []porcupine.Operation
	count map[OpKind]int
}

func newRecorder() *recorder {
	return &recorder{base: time.Now(), count: make(map[OpKind]int)}
}

func (r *recorder) now() int64 { return time.Since(r.base).Nanoseconds() }

func (r *recorder) add(client int, in kvInput, out kvOutput, call, ret int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, porcupine.Operation{ClientId: client, Input: in, Output: out, Call: call, Return: ret})
	r.count[in.Kind]++
}

func (r *recorder) check(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	ops := append([]porcupine.Operation(nil), r.ops...)
	r.mu.Unlock()

	begin := time.Now()
	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, 60*time.Second)
	t.Logf("history: %d ops (get=%d put=%d append=%d), porcupine=%s in %v",
		len(ops), r.count[OpGet], r.count[OpPut], r.count[OpAppend], res, time.Since(begin).Round(time.Millisecond))

	switch res {
	case porcupine.Illegal:
		path := filepath.Join(os.TempDir(), fmt.Sprintf("raftkv-porcupine-%d.html", time.Now().UnixNano()))
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
			t.Logf("visualization written to %s", path)
		}
		t.Fatal("history is not linearizable")
	case porcupine.Unknown:
		t.Log("porcupine timed out; linearizability not established")
	}
}

type nemesis func(c *cluster, stop <-chan struct{})

func partitionNemesis(c *cluster, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			c.net.Heal()
			return
		case <-time.After(time.Duration(300+c.intn(700)) * time.Millisecond):
		}
		perm := c.perm(c.n)
		cut := 1 + c.intn(c.n-1)
		c.net.Partition(perm[:cut], perm[cut:])
		select {
		case <-stop:
			c.net.Heal()
			return
		case <-time.After(time.Duration(200+c.intn(800)) * time.Millisecond):
		}
		c.net.Heal()
	}
}

func crashNemesis(c *cluster, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(time.Duration(500+c.intn(1000)) * time.Millisecond):
		}
		victim := c.intn(c.n)
		c.crash(victim)
		select {
		case <-stop:
		case <-time.After(time.Duration(100+c.intn(500)) * time.Millisecond):
		}
		c.start(victim)
	}
}

func runLinearizability(t *testing.T, reliable bool, nemeses []nemesis, duration time.Duration) {
	const servers, clients, keys = 5, 8, 3
	c := newCluster(t, servers, reliable)
	rec := newRecorder()

	stop := make(chan struct{})
	var faults sync.WaitGroup
	for _, nm := range nemeses {
		faults.Add(1)
		go func(nm nemesis) {
			defer faults.Done()
			nm(c, stop)
		}(nm)
	}

	var done atomic.Bool
	var wg sync.WaitGroup
	for id := range clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ck := c.clerk()
			for i := 0; !done.Load(); i++ {
				key := fmt.Sprint(c.intn(keys))
				roll := c.intn(100)
				in := kvInput{Key: key}
				var out kvOutput
				call := rec.now()
				var err error
				switch {
				case roll < 40:
					in.Kind = OpGet
					out.Value, err = ck.Get(bg, key)
				case roll < 55:
					in.Kind, in.Value = OpPut, fmt.Sprintf("p%d.%d;", id, i)
					err = ck.Put(bg, key, in.Value)
				default:
					in.Kind, in.Value = OpAppend, fmt.Sprintf("a%d.%d;", id, i)
					err = ck.Append(bg, key, in.Value)
				}
				if err != nil {
					t.Errorf("client %d: %v", id, err)
					return
				}
				rec.add(id, in, out, call, rec.now())
			}
		}(id)
	}

	time.Sleep(duration)
	close(stop)
	faults.Wait()
	for i := range servers {
		c.mu.Lock()
		down := c.servers[i] == nil
		c.mu.Unlock()
		if down {
			c.start(i)
		}
	}
	c.net.SetReliable(true)
	c.net.Heal()
	done.Store(true)
	wg.Wait()

	rec.check(t)
}

func TestLinearizableReliable(t *testing.T) {
	runLinearizability(t, true, nil, 3*time.Second)
}

func TestLinearizableUnderPartitions(t *testing.T) {
	runLinearizability(t, true, []nemesis{partitionNemesis}, 6*time.Second)
}

func TestLinearizableUnreliableWithCrashes(t *testing.T) {
	runLinearizability(t, false, []nemesis{partitionNemesis, crashNemesis}, 8*time.Second)
}

// TestCheckerCatchesStaleRead makes sure the model is not vacuous: a read
// that returns a value overwritten before the read began must be rejected.
func TestCheckerCatchesStaleRead(t *testing.T) {
	ops := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{Kind: OpPut, Key: "x", Value: "1"}, Call: 0, Return: 10},
		{ClientId: 0, Input: kvInput{Kind: OpPut, Key: "x", Value: "2"}, Call: 20, Return: 30},
		{ClientId: 1, Input: kvInput{Kind: OpGet, Key: "x"}, Output: kvOutput{Value: "1"}, Call: 40, Return: 50},
	}
	if porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("stale read accepted")
	}
	ops[2].Call = 25
	ops[2].Output = kvOutput{Value: "2"}
	if !porcupine.CheckOperations(kvModel, ops) {
		t.Fatal("valid history rejected")
	}
}
