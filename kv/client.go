package kv

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/ranadhir-rikkala/raftkv/transport"
)

// Clerk is a client of the replicated store. It remembers the last known
// leader and retries each operation, with the same (ClientID, Seq), until
// some server reports it applied. Operations on one Clerk are serialized.
type Clerk struct {
	mu          sync.Mutex
	servers     []transport.Endpoint
	leader      int
	clientID    int64
	seq         int64
	callTimeout time.Duration
}

func NewClerk(servers []transport.Endpoint) *Clerk {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return &Clerk{
		servers:     servers,
		clientID:    int64(binary.LittleEndian.Uint64(b[:]) >> 1),
		callTimeout: 1500 * time.Millisecond,
	}
}

func (ck *Clerk) ClientID() int64 { return ck.clientID }

// Get returns the current value of key, or "" if it does not exist.
func (ck *Clerk) Get(ctx context.Context, key string) (string, error) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++
	args := GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.seq}
	var value string
	err := ck.retry(ctx, func(ctx context.Context, ep transport.Endpoint) (Err, error) {
		var reply GetReply
		if err := ep.Call(ctx, "KV.Get", &args, &reply); err != nil {
			return "", err
		}
		value = reply.Value
		return reply.Err, nil
	})
	return value, err
}

func (ck *Clerk) Put(ctx context.Context, key, value string) error {
	return ck.putAppend(ctx, OpPut, key, value)
}

func (ck *Clerk) Append(ctx context.Context, key, value string) error {
	return ck.putAppend(ctx, OpAppend, key, value)
}

func (ck *Clerk) putAppend(ctx context.Context, kind OpKind, key, value string) error {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++
	args := PutAppendArgs{Kind: kind, Key: key, Value: value, ClientID: ck.clientID, Seq: ck.seq}
	return ck.retry(ctx, func(ctx context.Context, ep transport.Endpoint) (Err, error) {
		var reply PutAppendReply
		if err := ep.Call(ctx, "KV.PutAppend", &args, &reply); err != nil {
			return "", err
		}
		return reply.Err, nil
	})
}

// retry returns nil once a server answers OK or ErrNoKey. If ctx expires
// first the operation's outcome is unknown: it may still be applied later.
func (ck *Clerk) retry(ctx context.Context, attempt func(context.Context, transport.Endpoint) (Err, error)) error {
	for {
		for range ck.servers {
			if err := ctx.Err(); err != nil {
				return err
			}
			cctx, cancel := context.WithTimeout(ctx, ck.callTimeout)
			e, err := attempt(cctx, ck.servers[ck.leader])
			cancel()
			if err == nil && (e == OK || e == ErrNoKey) {
				return nil
			}
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
