package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := p.Load(); err != nil || len(data) != 0 {
		t.Fatalf("fresh Load = %q, %v", data, err)
	}
	for _, s := range []string{"first", "second, longer state", "3"} {
		if err := p.Save([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}

	reopened, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, err := reopened.Load()
	if err != nil || !bytes.Equal(data, []byte("3")) {
		t.Fatalf("reopened Load = %q, %v", data, err)
	}
	if reopened.Size() != 1 {
		t.Fatalf("Size = %d, want 1", reopened.Size())
	}
	if _, err := os.Stat(filepath.Join(dir, "raft-state.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
}

func TestRaftStateSurvivesFilePersister(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	rf := &Raft{persister: p, currentTerm: 7, votedFor: 2, log: []Entry{{}, {Term: 3, Command: []byte("x")}, {Term: 7, Command: []byte("y")}}}
	rf.persist()

	p2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := &Raft{persister: p2}
	if err := got.restore(); err != nil {
		t.Fatal(err)
	}
	if got.currentTerm != 7 || got.votedFor != 2 || len(got.log) != 3 || string(got.log[2].Command) != "y" {
		t.Fatalf("restored term=%d votedFor=%d log=%+v", got.currentTerm, got.votedFor, got.log)
	}
}
