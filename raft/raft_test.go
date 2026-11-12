package raft

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func cmd(v int) string { return strconv.Itoa(v) }

func TestInitialElection(t *testing.T) {
	h := newHarness(t, 3, true)
	h.checkOneLeader()

	time.Sleep(50 * time.Millisecond)
	term1 := h.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, want >= 1", term1)
	}

	time.Sleep(2 * electionTimeout)
	if term2 := h.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	h.checkOneLeader()
}

func TestReElection(t *testing.T) {
	h := newHarness(t, 3, true)
	leader1 := h.checkOneLeader()

	h.disconnect(leader1)
	h.checkOneLeader()

	h.connect(leader1)
	leader2 := h.checkOneLeader()

	h.disconnect(leader2)
	h.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * electionTimeout)
	h.checkNoLeader()

	h.connect((leader2 + 1) % 3)
	h.checkOneLeader()

	h.connect(leader2)
	h.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	const n = 7
	h := newHarness(t, n, true)
	h.checkOneLeader()

	for range 10 {
		i1, i2, i3 := h.rng.Intn(n), h.rng.Intn(n), h.rng.Intn(n)
		h.disconnect(i1)
		h.disconnect(i2)
		h.disconnect(i3)
		h.checkOneLeader()
		h.connect(i1)
		h.connect(i2)
		h.connect(i3)
	}
	h.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	h := newHarness(t, 3, true)
	for index := 1; index <= 3; index++ {
		if c, _ := h.nCommitted(index); c > 0 {
			t.Fatal("entry committed before Start")
		}
		if got := h.one(cmd(index*100), 3, false); got != index {
			t.Fatalf("got index %d, want %d", got, index)
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	h := newHarness(t, 3, true)
	h.one("101", 3, false)

	leader1 := h.checkOneLeader()
	h.disconnect((leader1 + 1) % 3)
	h.one("102", 2, false)
	time.Sleep(electionTimeout)
	h.one("103", 2, false)

	leader2 := h.checkOneLeader()
	h.disconnect((leader2 + 1) % 3)
	h.disconnect((leader2 + 2) % 3)

	index, _, ok := h.raft(leader2).Start([]byte("104"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 4 {
		t.Fatalf("got index %d, want 4", index)
	}
	time.Sleep(2 * electionTimeout)
	if c, _ := h.nCommitted(index); c > 0 {
		t.Fatalf("%d servers committed without a majority", c)
	}
}

func TestLeaderFailure(t *testing.T) {
	h := newHarness(t, 3, true)
	h.one("101", 3, false)

	leader1 := h.checkOneLeader()
	h.disconnect(leader1)
	h.one("102", 2, false)
	time.Sleep(electionTimeout)
	h.one("103", 2, false)

	leader2 := h.checkOneLeader()
	h.disconnect(leader2)

	for i := range 3 {
		if rf := h.raft(i); rf != nil {
			rf.Start([]byte("104"))
		}
	}
	time.Sleep(2 * electionTimeout)
	if c, _ := h.nCommitted(4); c > 0 {
		t.Fatalf("%d servers committed with no quorum", c)
	}
}

func TestRejoinAfterFollowerFailure(t *testing.T) {
	h := newHarness(t, 3, true)
	h.one("101", 3, false)

	leader := h.checkOneLeader()
	h.disconnect((leader + 1) % 3)
	h.one("102", 2, false)
	h.one("103", 2, false)
	time.Sleep(electionTimeout)
	h.one("104", 2, false)
	h.one("105", 2, false)

	h.connect((leader + 1) % 3)
	h.one("106", 3, true)
	time.Sleep(electionTimeout)
	h.one("107", 3, true)
}

func TestNoAgreementWithoutQuorum(t *testing.T) {
	const n = 5
	h := newHarness(t, n, true)
	h.one("10", n, false)

	leader := h.checkOneLeader()
	h.disconnect((leader + 1) % n)
	h.disconnect((leader + 2) % n)
	h.disconnect((leader + 3) % n)

	index, _, ok := h.raft(leader).Start([]byte("20"))
	if !ok {
		t.Fatal("leader rejected Start")
	}
	if index != 2 {
		t.Fatalf("got index %d, want 2", index)
	}
	time.Sleep(2 * electionTimeout)
	if c, _ := h.nCommitted(index); c > 0 {
		t.Fatalf("%d servers committed without a majority", c)
	}

	h.connect((leader + 1) % n)
	h.connect((leader + 2) % n)
	h.connect((leader + 3) % n)

	leader2 := h.checkOneLeader()
	index2, _, ok := h.raft(leader2).Start([]byte("30"))
	if !ok {
		t.Fatal("leader2 rejected Start")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %d", index2)
	}
	h.one("1000", n, true)
}

func TestConcurrentStarts(t *testing.T) {
	h := newHarness(t, 3, true)
	var success bool
	for attempt := 0; attempt < 5 && !success; attempt++ {
		if attempt > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := h.checkOneLeader()
		rf := h.raft(leader)
		_, term, ok := rf.Start([]byte("1"))
		if !ok {
			continue
		}

		var wg sync.WaitGroup
		indices := make(chan int, 5)
		for i := range 5 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				index, term1, ok := rf.Start([]byte(cmd(100 + i)))
				if ok && term1 == term {
					indices <- index
				}
			}(i)
		}
		wg.Wait()
		close(indices)

		if !h.termStable(term) {
			continue
		}
		var cmds []string
		failed := false
		for index := range indices {
			v := h.waitCommitted(index, 3, term)
			if v == "" {
				failed = true
				break
			}
			cmds = append(cmds, v)
		}
		if failed {
			continue
		}
		seen := make(map[string]bool)
		for _, c := range cmds {
			seen[c] = true
		}
		for i := range 5 {
			if !seen[cmd(100+i)] {
				t.Fatalf("command %d missing from %v", 100+i, cmds)
			}
		}
		success = true
	}
	if !success {
		t.Fatal("term changed too often")
	}
}

func (h *harness) termStable(term int) bool {
	for i := range h.n {
		if rf := h.raft(i); rf != nil {
			if t, _ := rf.State(); t != term {
				return false
			}
		}
	}
	return true
}

func TestRejoinOfPartitionedLeader(t *testing.T) {
	h := newHarness(t, 3, true)
	h.one("101", 3, true)

	leader1 := h.checkOneLeader()
	h.disconnect(leader1)
	rf1 := h.raft(leader1)
	rf1.Start([]byte("102"))
	rf1.Start([]byte("103"))
	rf1.Start([]byte("104"))

	h.one("103", 2, true)

	leader2 := h.checkOneLeader()
	h.disconnect(leader2)

	h.connect(leader1)
	h.one("104", 2, true)

	h.connect(leader2)
	h.one("105", 3, true)
}

// TestBackup forces a leader to roll followers back over long runs of
// conflicting entries from older terms.
func TestBackup(t *testing.T) {
	const n = 5
	h := newHarness(t, n, true)
	h.one(cmd(h.rng.Int()), n, true)

	leader1 := h.checkOneLeader()
	h.disconnect((leader1 + 2) % n)
	h.disconnect((leader1 + 3) % n)
	h.disconnect((leader1 + 4) % n)
	for range 50 {
		h.raft(leader1).Start([]byte(cmd(h.rng.Int())))
	}
	time.Sleep(electionTimeout / 2)

	h.disconnect((leader1 + 0) % n)
	h.disconnect((leader1 + 1) % n)
	h.connect((leader1 + 2) % n)
	h.connect((leader1 + 3) % n)
	h.connect((leader1 + 4) % n)
	for range 50 {
		h.one(cmd(h.rng.Int()), 3, true)
	}

	leader2 := h.checkOneLeader()
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	h.disconnect(other)
	for range 50 {
		h.raft(leader2).Start([]byte(cmd(h.rng.Int())))
	}
	time.Sleep(electionTimeout / 2)

	for i := range n {
		h.disconnect(i)
	}
	h.connect((leader1 + 0) % n)
	h.connect((leader1 + 1) % n)
	h.connect(other)
	for range 50 {
		h.one(cmd(h.rng.Int()), 3, true)
	}

	for i := range n {
		h.connect(i)
	}
	h.one(cmd(h.rng.Int()), n, true)
}

func TestPersistAfterFullRestart(t *testing.T) {
	h := newHarness(t, 3, true)
	h.one("11", 3, true)

	for i := range 3 {
		h.start(i)
	}
	for i := range 3 {
		h.disconnect(i)
		h.connect(i)
	}
	h.one("12", 3, true)

	leader1 := h.checkOneLeader()
	h.disconnect(leader1)
	h.start(leader1)
	h.connect(leader1)
	h.one("13", 3, true)

	leader2 := h.checkOneLeader()
	h.disconnect(leader2)
	h.one("14", 2, true)
	h.start(leader2)
	h.connect(leader2)
	h.waitCommitted(4, 3, -1)

	i3 := (h.checkOneLeader() + 1) % 3
	h.disconnect(i3)
	h.one("15", 2, true)
	h.start(i3)
	h.connect(i3)
	h.one("16", 3, true)
}

func TestPersistUnderPartitionsAndCrashes(t *testing.T) {
	const n = 5
	h := newHarness(t, n, true)
	index := 1
	for range 5 {
		h.one(cmd(10+index), n, true)
		index++

		leader1 := h.checkOneLeader()
		h.disconnect((leader1 + 1) % n)
		h.disconnect((leader1 + 2) % n)
		h.one(cmd(10+index), n-2, true)
		index++

		h.disconnect((leader1 + 0) % n)
		h.disconnect((leader1 + 3) % n)
		h.disconnect((leader1 + 4) % n)

		h.start((leader1 + 1) % n)
		h.start((leader1 + 2) % n)
		h.connect((leader1 + 1) % n)
		h.connect((leader1 + 2) % n)
		time.Sleep(electionTimeout)

		h.start((leader1 + 3) % n)
		h.connect((leader1 + 3) % n)
		h.one(cmd(10+index), n-2, true)
		index++

		h.connect((leader1 + 4) % n)
		h.connect((leader1 + 0) % n)
	}
	h.one("1000", n, true)
}

// TestFigure8 repeatedly crashes leaders right after they append entries, so
// that a new leader may see entries from older terms replicated on a
// majority. Figure 8 of the Raft paper shows why such entries must not be
// committed by counting replicas alone.
func TestFigure8(t *testing.T) {
	const n = 5
	h := newHarness(t, n, true)
	h.one(cmd(h.rng.Int()), 1, true)

	up := n
	for range 300 {
		leader := -1
		for i := range n {
			if rf := h.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(cmd(h.rng.Int()))); ok {
					leader = i
				}
			}
		}
		if h.rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(h.rng.Intn(int(electionTimeout/time.Millisecond)/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(h.rng.Intn(13)) * time.Millisecond)
		}
		if leader != -1 {
			h.crash(leader)
			up--
		}
		if up < 3 {
			s := h.rng.Intn(n)
			if h.raft(s) == nil {
				h.start(s)
				h.connect(s)
				up++
			}
		}
	}
	for i := range n {
		if h.raft(i) == nil {
			h.start(i)
			h.connect(i)
		}
	}
	h.one(cmd(h.rng.Int()), n, true)
}

func TestUnreliableAgree(t *testing.T) {
	const n = 5
	h := newHarness(t, n, false)
	var wg sync.WaitGroup
	for iter := 1; iter < 50; iter++ {
		for j := range 4 {
			wg.Add(1)
			go func(iter, j int) {
				defer wg.Done()
				h.one(fmt.Sprintf("%d-%d", iter, j), 1, true)
			}(iter, j)
		}
		h.one(cmd(iter), 1, true)
	}
	h.net.SetReliable(true)
	wg.Wait()
	h.one("100", n, true)
}

// TestFigure8Unreliable is TestFigure8 on a network that drops, delays and
// reorders messages, with long-delayed replies arriving after newer ones.
func TestFigure8Unreliable(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const n = 5
	h := newHarness(t, n, false)
	h.one(cmd(h.rng.Intn(10000)), 1, true)

	nup := n
	for iter := range 500 {
		if iter == 100 {
			h.net.SetLongReordering(true)
		}
		leader := -1
		for i := range n {
			if rf := h.raft(i); rf != nil {
				if _, _, ok := rf.Start([]byte(cmd(h.rng.Intn(10000)))); ok && h.isConnected(i) {
					leader = i
				}
			}
		}
		if h.rng.Intn(1000) < 100 {
			time.Sleep(time.Duration(h.rng.Intn(int(electionTimeout/time.Millisecond)/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(h.rng.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && h.rng.Intn(1000) < int(electionTimeout/time.Millisecond)/2 {
			h.disconnect(leader)
			nup--
		}
		if nup < 3 {
			s := h.rng.Intn(n)
			if !h.isConnected(s) {
				h.connect(s)
				nup++
			}
		}
	}
	for i := range n {
		h.connect(i)
	}
	h.one(cmd(h.rng.Intn(10000)), n, true)
}
