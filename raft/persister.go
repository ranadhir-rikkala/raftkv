package raft

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Persister is stable storage for Raft's durable state (currentTerm,
// votedFor and the log). Save must not return until the state would survive
// a crash.
type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error)
	Size() int
}

// MemoryPersister keeps state in memory. Tests "crash" a node by taking a
// Copy and handing the copy to the restarted node.
type MemoryPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemoryPersister() *MemoryPersister { return &MemoryPersister{} }

func (p *MemoryPersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	return nil
}

func (p *MemoryPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

func (p *MemoryPersister) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

func (p *MemoryPersister) Copy() *MemoryPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemoryPersister{state: append([]byte(nil), p.state...)}
}

// FilePersister stores state in a single file, replaced atomically by
// writing a temp file, fsyncing it, renaming it over the old one and fsyncing
// the directory.
type FilePersister struct {
	mu   sync.Mutex
	path string
	size int
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &FilePersister{path: filepath.Join(dir, "raft-state")}
	info, err := os.Stat(p.path)
	switch {
	case err == nil:
		p.size = int(info.Size())
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	return p, nil
}

func (p *FilePersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(state); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(p.path)); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}
	p.size = len(state)
	return nil
}

func (p *FilePersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := os.ReadFile(p.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return data, err
}

func (p *FilePersister) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
