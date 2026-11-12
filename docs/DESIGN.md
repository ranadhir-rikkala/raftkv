# raftkv design notes

This document explains the decisions behind milestone 1: a Raft library, a
key-value state machine on top of it, and the test infrastructure used to
break both.

## Layout

| Package | Responsibility |
|---|---|
| `transport` | `Endpoint` interface: `Call(ctx, method, args, reply) error` |
| `transport/memnet` | In-process network for tests, with fault injection |
| `transport/rpcnet` | TCP transport using `net/rpc` + gob |
| `raft` | Consensus: election, replication, commitment, persistence |
| `kv` | Replicated map, client `Clerk`, duplicate detection |
| `cmd/raftkv-server`, `cmd/raftkv-cli` | Binaries for a real cluster |

Raft never imports a concrete transport. It is handed `[]transport.Endpoint`
and exposes its RPC handlers as ordinary methods with the `net/rpc` shape
(`func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error`).
Both `memnet` and `rpcnet` dispatch to the same methods, so the code under test
is the code that runs in the cluster.

## Concurrency model

Each `Raft` has one mutex that guards all of its state. I chose a single lock
over finer-grained locking because almost every Raft rule reads several fields
together (term, role, log, commit index), and a coarse lock makes each handler
an atomic step that is easy to reason about against Figure 2.

Long-running goroutines per node:

- **ticker** wakes every 10 ms. A leader sends heartbeats when its 50 ms
  heartbeat deadline passes; a follower or candidate starts an election when
  its randomized deadline (250 to 500 ms) passes.
- **applier** waits on a `sync.Cond` until `commitIndex > lastApplied`, copies
  the newly committed entries while holding the lock, releases the lock and
  then sends them on the apply channel. It also selects on a `done` channel
  so a killed node never blocks forever on a channel nobody reads.

Short-lived goroutines are spawned per outgoing RPC (one per peer per
heartbeat or `Start`, one per peer per election).

Rules that keep this correct:

1. **Never hold the lock across an RPC or a channel send.** Arguments are built
   under the lock (log entries are copied, not aliased), the lock is released,
   the call is made, and the lock is reacquired to process the reply.
2. **Re-validate after reacquiring.** A reply is only acted on if the node is
   still in the role and term that sent the request
   (`rf.role == Leader && rf.currentTerm == args.Term`). Any reply with a
   higher term converts the node to follower first.
3. **Stale replies must not move state backwards.** `matchIndex` only grows.
   On a rejected AppendEntries, `nextIndex` is only rewound if it still equals
   the value the request was built from, so an old rejection that arrives late
   cannot undo progress made by a newer success.
4. **Followers never shrink `commitIndex`.** A reordered AppendEntries with an
   older `LeaderCommit` is ignored for commit purposes, and entries that already
   match are not truncated (truncation only happens at the first conflicting
   term).

All tests run under `-race`.

## Log replication details

- **Accelerated backtracking.** On a consistency-check failure the follower
  returns `ConflictTerm` (the term at `PrevLogIndex`, or -1 if its log is too
  short), `ConflictIndex` (first index of that term) and `LogLen`. The leader
  jumps to just after its own last entry of `ConflictTerm` if it has one, or to
  `ConflictIndex` otherwise. `TestBackup` needs this to finish quickly: it
  leaves followers with 50-entry runs of conflicting entries.
- **Figure 8 rule.** The leader only advances `commitIndex` to an index `N`
  whose entry is from its *current* term and whose `matchIndex` is held by a
  majority. Earlier-term entries become committed indirectly. `TestFigure8`
  and `TestFigure8Unreliable` exercise the case where counting replicas alone
  would commit an entry that a later leader overwrites.
- **No leader no-op.** A new leader does not append a no-op entry. The KV layer
  routes every operation (including reads) through the log, so the first client
  operation in a new term commits everything before it. A no-op will be needed
  for read-index reads (see roadmap).

## Persistence

Persistent state is `currentTerm`, `votedFor` and the log, exactly as Figure 2
lists. It is saved under the lock **before** the handler returns, so a peer
never sees a vote or an acknowledgement that the voter could forget after a
crash. Saves happen when a term changes, a vote is cast, entries are appended
by `Start`, or a follower's log is modified.

`Persister` is an interface with two implementations:

- `MemoryPersister` for tests. Crashing a node in the test harness takes a
  `Copy()` of its persister, so writes from the dying instance cannot leak
  into the restarted one.
- `FilePersister` for real nodes. It writes to `raft-state.tmp`, fsyncs it,
  renames it over `raft-state` and fsyncs the directory, so a crash leaves
  either the old or the new state, never a torn file.

If saving fails the node panics. Continuing after an unpersisted vote or append
would violate safety, so fail-stop is the only correct option.

**Known cost:** the whole state is gob-encoded on every save, so persistence is
O(log size) per write and the file grows without bound. Snapshots and log
compaction (InstallSnapshot) are the first roadmap item and fix both.

## KV state machine

Every operation, including `Get`, is a Raft log entry. That gives
linearizable reads with no extra machinery at the cost of a round of
replication per read.

Operation flow on the leader:

1. The handler checks the dedup table (below). If the op already ran, it
   returns the cached result immediately.
2. It calls `rf.Start(encodeOp(op))` while holding the KV lock, and registers a
   waiter keyed by the returned log index, tagged with `(ClientID, Seq)`.
   Registering under the same lock the applier takes avoids a race where the
   entry is applied before anyone is waiting for it.
3. The apply loop executes committed entries in order. When it reaches an
   index with a waiter it checks the tag. A different op at that index means
   this node lost leadership and its entry was overwritten, so the waiter gets
   `ErrWrongLeader`.
4. While waiting, the handler polls `rf.State()` every 50 ms and gives up as
   soon as the term changes or the node stops being leader, rather than waiting
   for the 1 s timeout.

Operations are encoded with a small varint-based binary format
(`kv/codec.go`) instead of gob, since gob repeats type information in every
standalone message.

## Duplicate detection (exactly-once semantics)

The client library (`Clerk`) assigns itself a random 63-bit `ClientID` and
numbers its operations with a sequence number that increases by one per
operation. A `Clerk` has at most one operation outstanding and retries it
with the *same* `(ClientID, Seq)` across servers until one reports success.

The state machine keeps a session table:

```
sessions[ClientID] = { Seq: last applied seq, Value, Err: that op's result }
```

When an entry is applied:

- `Seq > sessions[ClientID].Seq`: execute it and record the result.
- `Seq == sessions[ClientID].Seq`: a retry of the last op. Do not execute;
  return the cached result. This is what stops a retried `Append` whose first
  attempt committed (but whose reply was lost) from appending twice.
- `Seq < sessions[ClientID].Seq`: impossible for a well-behaved client, ignored.

Design points:

- The table is part of the replicated state machine, updated only by applied
  entries, so every replica makes identical dedup decisions and the table is
  rebuilt by replaying the log after a restart (`TestRestartReplaysLog`).
- Two copies of the same op can both be in the log (the client retried on a
  new leader before learning the first commit). The second is a no-op when
  applied.
- Only the last result per client is kept, which is enough because a client
  never has two operations in flight.
- Cached `Get` results are linearizable: the cached value was produced by an
  application of this same operation, which happened after the client first
  invoked it.

**Known gap:** sessions never expire, so the table grows with the number of
distinct clients ever seen. The Raft dissertation (section 6.3) describes
expiring sessions with leader-assigned timestamps carried in the log; that is
on the roadmap.

## Test network (`memnet`)

The in-memory network is what makes fault injection practical:

- **Isolation:** arguments and replies are gob-encoded across the call, so
  sender and receiver never share memory (and gob-incompatible types fail in
  tests, not in production).
- **Faults:** per-node disconnect, partitions into arbitrary groups, 10% request
  loss and 10% reply loss with 0 to 27 ms delays (`SetReliable(false)`), long
  delays for unreachable peers, and long reordering, where most replies are held
  for 200 ms to 2.2 s so newer replies overtake older ones.
- **Crash fencing:** each node has a generation number. Restarting a node bumps
  it, which invalidates every endpoint the previous incarnation held, so a
  killed node's goroutines cannot inject messages into the cluster.
- **Seeds:** the fault schedule and test randomness come from `RAFTKV_SEED`
  (logged by every test). Timers and goroutine scheduling still come from the
  real runtime, so a seed makes a failure more likely to recur but does not
  guarantee it. That is why the README calls the harness "deterministic-ish".

## Linearizability checking

KV tests under faults record every completed operation with call and return
timestamps from a monotonic clock, then check the history with
[porcupine](https://github.com/anishathalye/porcupine) against a
single-register-per-key model (histories are partitioned by key). A self-check
test feeds the model a hand-written stale read to make sure the checker is not
vacuous.

Clients in these tests retry until success (no abandoned operations), and the
test heals the network before stopping, so every recorded operation has a
definite outcome.
