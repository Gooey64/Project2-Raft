# Unit tests for the Raft node states

This document describes the unit tests for:

- `node_follower_state.go`
- `node_leader_state.go`
- `node_candidate_state.go`
- `randomTimeout` in `util.go`

There are 69 top-level test functions, plus table-driven subtests.

## Running

```sh
cd pkg
go test .                                   # run everything
go test -run 'Candidate|RequestVotes' .      # one area
go test -coverprofile=cover.out . && go tool cover -func=cover.out
go tool cover -html=cover.out               # line-by-line view
```

### Assumptions

The candidate tests and the `randomTimeout` tests specify how a **correct**
implementation behaves (Raft paper, figure 2 and section 5.2). They do not
describe the current TODO stubs. Until `node_candidate_state.go` and
`randomTimeout` are implemented, these tests fail, and that is expected:

- every `TestDoCandidate_*`, `TestRequestVotes*` and `TestCandidateHandleRequestVote` test
- every `TestRandomTimeout_*` test
- the four follower timer tests: `TestDoFollower_ElectionTimeoutBecomesCandidate`,
  `TestDoFollower_HeartbeatsResetTimeout`, `TestDoFollower_GrantedVoteResetsTimeout`,
  `TestDoFollower_StaleMessagesDoNotResetTimeout`

Against the current stubs, every leader test and every other follower test
passes. The tests were also checked against two different reference
implementations of the candidate and `randomTimeout`, written in a throwaway
copy and not part of this repo. With those, the whole suite passed 8 times in
a row, and coverage was 100% for all three state files and `randomTimeout`.

### Design choices the tests allow

Where the starter code leaves more than one valid design, the tests accept
any of them:

| Behavior | Accepted |
|---|---|
| Candidate loses or splits an election and times out | It either loops inside `doCandidate` and starts a new election, or returns `n.doCandidate` |
| Candidate receives a client request | It replies `NOT_LEADER` or `ELECTION_IN_PROGRESS` (the same pair `test/example_client_test.go` accepts) |
| `requestVotes` | It may contact peers in parallel or one at a time; the fake peers are safe for concurrent use |
| `doCandidate` | It may call `requestVotes` synchronously or in a goroutine |

### Behavior the tests require

These are the points where a "reasonable" implementation could differ from
the tests. Check these first if a test fails.

- A candidate's `handleRequestVote` with a **higher** term updates the term,
  replies to the request (granting the vote only if the candidate's log is up
  to date), and returns `fallback = true`. The leader already handles this case
  the same way, via `followerHandleRequestVote`.
- A candidate's `handleRequestVote` with an equal or lower term rejects the
  vote and returns `fallback = false`.
- `requestVotes` counts its own vote, does not send an RPC to itself, treats
  unreachable peers as "no", and needs a strict majority (2 of 4 loses).
- When `requestVotes` sees a higher term in a reply, it updates the term,
  clears `votedFor`, and returns `(true, false)`.
- `randomTimeout(min)` fires once after a random delay in `[min, 2*min]`
  (with ~40ms of scheduling slack above), and returns a new channel per call.

## Files

| File | Contents |
|---|---|
| `node_test_helpers_test.go` | Shared helpers (no tests) |
| `node_follower_state_test.go` | Follower: vote handling, AppendEntries handling, follower loop |
| `node_leader_state_test.go` | Leader: client requests, step-down, heartbeats/commit, log application, leader loop |
| `node_candidate_state_test.go` | Candidate: election loop, `requestVotes`, candidate vote handling |
| `util_test.go` | `randomTimeout` |

## Test infrastructure (`node_test_helpers_test.go`)

| Helper | Purpose |
|---|---|
| `newTestNode` | Real node (gRPC server and in-memory store) with a fake state machine. On cleanup it cancels the node and stops its server. It **never** calls `n.Exit()`, because that calls `os.Exit(0)` and would silently end the test binary with a passing status. |
| `fakeStateMachine` | Records applied commands and returns `"ok:<data>"`. Setting `fail = true` makes it return an error. |
| `fakePeer` / `fakeVoter` | A real gRPC node whose `appendEntries` / `requestVote` channels are answered by a test-supplied handler instead of a state function. A leader or candidate under test talks to it over the real RPC path. |
| `voteRecorder` | A concurrency-safe RequestVote handler. It records the requests it receives and replies with a fixed grant decision, optionally at a forced higher term. |
| `deniedPeer` | A peer that the network policy blocks, so RPCs to it fail immediately without dialing. Used to model unreachable nodes. |
| `runState` / `waitNext` / `assertStillRunning` | Run a state function in a goroutine, wait for the next state it returns, or check that it is still running. |
| `sameState` | Compares two state functions (for example, "did it return `n.doFollower`?") using `reflect`. |
| `sendAE` / `sendRV` / `sendClient` / `sendExit` | Deliver a message to a running state function. |
| `waitAEReply` / `waitRVReply` / `waitClientReply` | Wait for the reply to a delivered message. |
| `eventually` | Polls a condition until it is true or the wait times out. |
| `expectPanic` | Fails unless the function panics. |
| `storeLogs` | Appends NOOP entries with the given terms at indices 1..n. |

Every blocking wait is bounded by `waitTimeout` (3s). A bug shows up as a test
failure, not a hung test run.

---

## Follower tests (`node_follower_state_test.go`)

### `followerHandleRequestVote`: `TestFollowerRequestVote` (table)

Every case checks: the vote grant, `resetTimeout` (true only when the vote is
granted), `fallback`, the reply term, the stored term, and `votedFor`.

| Case | Setup | Request | Expected |
|---|---|---|---|
| lower term rejected | term 5 | term 4 | reject, no fallback, term stays 5 |
| same term, not yet voted | term 2 | term 2 | grant, `votedFor = cand` |
| already voted for someone else | term 2, voted "other" | term 2 | reject, `votedFor` stays "other" |
| already voted for same candidate | term 2, voted "cand" | term 2 | grant again (the vote is idempotent) |
| higher term clears previous vote | term 2, voted "other" | term 3 | term becomes 3, vote cleared, then granted |
| higher term but older last-log term | log terms [1,2] | term 5, lastLogTerm 1 | reject; term updated, `votedFor` cleared |
| higher last-log term, shorter log | log [1,1] | lastLogTerm 3, lastIdx 1 | grant (a newer term beats a longer log) |
| same last term, shorter log | log [1,1] | lastLogTerm 1, lastIdx 1 | reject |
| same last term, equal length | log [1,1] | lastLogTerm 1, lastIdx 2 | grant |
| same last term, longer log | log [1,1] | lastLogTerm 1, lastIdx 7 | grant |

### `handleAppendEntries`

| Test | Verifies |
|---|---|
| `LowerTermRejected` | A stale term is rejected with our term. It does not reset the timer, fall back, set the leader, or touch `lastHeardFromLeader`. |
| `HeartbeatSetsLeaderAndTerm` | A higher-term heartbeat succeeds, sets reset and fallback, records the leader, adopts the term, clears `votedFor`, and updates `lastHeardFromLeader`. |
| `SameTermKeepsVote` | A same-term request does not clear `votedFor`. |
| `LeaderChange` | The leader is unchanged if the same leader sends again, and replaced when a different leader sends. |
| `PrevLogIndexTooHigh` | Rejected when `PrevLogIndex > LastLogIndex`. The log is untouched, but reset and fallback are still true (the leader is valid). |
| `PrevLogTermMismatch` | Rejected when the entry at `PrevLogIndex` has a different term. The log is untouched. |
| `AppendsNewEntries` | New entries are appended. The commit index is unchanged when `LeaderCommit = 0`. |
| `TruncatesConflictingEntries` | A conflicting suffix is removed and replaced with the leader's entry. |
| `PanicsOnLogGap` | Entries that do not start at `LastLogIndex+1` trigger the sanity-check panic. |
| `AdvancesCommitIndex` | `CommitIndex` and `LastApplied` advance to `LeaderCommit`. |
| `CommitIndexCappedAtLastLogIndex` | The commit index becomes `min(LeaderCommit, LastLogIndex)`. |
| `AppliesCommittedCommands` | Committed registration and command entries reach the state machine, and their replies are cached. |
| `LeaderCommitNotAhead` | The commit index never moves backwards. |

### `doFollower` loop

| Test | Verifies |
|---|---|
| `SetsStateAndExitsOnShutdown` | The state becomes `FollowerState`. `gracefulExit <- false` is ignored, and `true` returns `nil`. |
| `HandlesAppendEntries` | Valid and stale AppendEntries get correct replies, and the loop keeps running. |
| `HandlesRequestVote` | The first vote of a term is granted, a second candidate in the same term is refused, and the loop keeps running. |
| `RejectsClientRequests` | Clients get `NOT_LEADER` with the known leader as the hint. |
| `ElectionTimeoutBecomesCandidate` | With no messages, the follower returns `n.doCandidate`. |
| `HeartbeatsResetTimeout` | Heartbeats every 20ms for 700ms (timeout 150–300ms) keep the node a follower. Once they stop, it becomes a candidate. |
| `GrantedVoteResetsTimeout` | Same as above, using granted votes (a new term each time). |
| `StaleMessagesDoNotResetTimeout` | Stale AppendEntries, refused votes and client requests do **not** keep the follower alive. It still times out. |

Loop tests that are not about timing use a one-hour election timeout, so they
never drift into the candidate state.

---

## Leader tests (`node_leader_state_test.go`)

### `leaderHandleClientRequest`

| Test | Verifies |
|---|---|
| `Registration` | `ClientId 0` appends a `CLIENT_REGISTRATION` entry at the next index with the current term and cache ID `0-<index>`, updates `matchIndex[self]`, and registers the reply channel without answering it yet. |
| `NewCommand` | A `STATE_MACHINE_COMMAND` entry carries the command, data and cache ID `<client>-<seq>`, and the reply is registered. |
| `CachedReply` | An already-applied request gets its cached reply immediately and is not appended. |
| `DuplicatePending` | A retry of an in-flight request adds a second waiter but no second log entry. |

### `stepDownFromLeader`

| Test | Verifies |
|---|---|
| `StepDownFromLeader` | Every waiting client (several per cache ID, several IDs) gets the given status plus a leader hint, and the pending map is emptied. |
| `NoPending` | Does not block or panic when nothing is pending. |

### `sendHeartbeats`

| Test | Verifies |
|---|---|
| `SingleNodeCommitsImmediately` | A cluster of one commits and applies current-term entries right away. |
| `DoesNotCommitOldTermEntries` | Entries from an older term are not committed by counting replicas (Raft §5.4.2). |
| `NoMajorityNoCommit` | With unreachable peers there is no commit and no fallback, and `nextIndex` is unchanged. |
| `SuccessReplicatesAndCommits` | The request has the right `PrevLogIndex`/`PrevLogTerm`, entries, leader and term. On success, `matchIndex`/`nextIndex` advance, and the entry commits once 2 of 3 have it. |
| `FailureDecrementsNextIndex` | A rejection decrements `nextIndex`, which never goes below 1. |
| `HigherTermFallsBack` | A higher-term reply returns `true`, adopts the term, clears `votedFor`, and commits nothing. |

### `processLogEntry`

| Test | Verifies |
|---|---|
| `Noop` | A NOOP is a no-op and returns `false`. |
| `Registration` | The client gets OK with `ClientId = log index` and a self leader hint. The reply is cached, and the pending entry is removed. |
| `CommandSuccess` | The state machine is applied, and every waiter gets OK, the client ID and the response. |
| `CommandFailure` | A state machine error becomes `REQ_FAILED` with the error text. |
| `NoWaitingClient` | Follower path: the reply is cached even when no one is waiting. |
| `InvalidCacheIDPanics` | A non-numeric client ID in the cache ID panics. |
| `AlreadyCachedPanics` | **Documents a probable bug.** If a command's reply is already cached, `processLogEntry` reuses it, then calls `CacheClientReply` again, which errors and panics. If you fix this, change the test to expect a normal reply. |

### `doLeader` loop

| Test | Verifies |
|---|---|
| `SingleNodeLifecycle` | It appends a NOOP for its term and sets itself as leader and state. Registration and commands are committed on the heartbeat tick and answered. Stale and same-term votes and stale AppendEntries are refused without stepping down. `gracefulExit false` is ignored, and `true` returns `nil`. |
| `InitializesPeerIndices` | `nextIndex = lastIndex+1` and `matchIndex = 0` for each peer, and `matchIndex[self]` points at the NOOP. |
| `StepsDownOnHigherTermAppendEntries` | Returns `doFollower`. Waiting clients get `NOT_LEADER` with the new leader's hint, and the term is updated. |
| `StepsDownOnHigherTermRequestVote` | Grants the vote, returns `doFollower`, and waiting clients get `ELECTION_IN_PROGRESS`. |
| `InitialHeartbeatFallback` | A higher term on the very first heartbeat causes an immediate step-down, and pending clients are notified. |
| `TickerHeartbeatFallback` | It succeeds for a few periodic heartbeats (so the NOOP commits), then steps down when a later heartbeat sees a higher term. |

---

## Candidate tests (`node_candidate_state_test.go`)

Most loop tests use `stuckCandidate`: a 3-node cluster with two unreachable
peers and a one-hour timeout, so the candidate sits in its election loop
waiting for votes that never arrive.

### `doCandidate`: running the election

| Test | Verifies |
|---|---|
| `StartsElection` | The state becomes `CandidateState`, the term goes up by one, and the node votes for itself. |
| `SingleNodeWins` | A cluster of one returns `n.doLeader` at the new term. |
| `WinsWithMajority` | With all votes it returns `doLeader`. Each peer got a RequestVote with the new term, the candidate's ID, and the correct `LastLogIndex`/`LastLogTerm`. |
| `WinsDespiteUnreachablePeer` | 2 of 3 votes (one peer unreachable) is enough to win. |
| `HigherTermVoteReplyFallsBack` | A vote reply with a higher term returns `doFollower` and adopts the term. |
| `LostElectionRetriesWithHigherTerm` | When all peers refuse, it times out and starts another election at term +1 (peers see the new term), and stays a candidate. |

### `doCandidate`: messages received while waiting for votes

| Test | Verifies |
|---|---|
| `AppendEntriesFromNewLeaderFallsBack` | A same-term leader's AppendEntries is accepted, the leader is recorded, and it returns `doFollower`. |
| `HigherTermAppendEntriesFallsBack` | A higher-term AppendEntries is accepted and it returns `doFollower`. |
| `StaleAppendEntriesRejected` | A lower-term AppendEntries is refused and it stays a candidate. |
| `HigherTermRequestVoteFallsBack` | Grants the vote to a higher-term candidate, updates term and `votedFor`, and returns `doFollower`. |
| `RivalSameTermRequestVoteRejected` | Refuses a rival in the same term (it already voted for itself) and stays a candidate. |
| `ClientRequestRejected` | Replies `NOT_LEADER` or `ELECTION_IN_PROGRESS` and stays a candidate. |
| `GracefulExit` | `false` is ignored, and `true` returns `nil`. |

### `requestVotes`: `TestRequestVotes` (table)

Each case runs with the node already a candidate at term 2 that has voted for
itself, and checks the `(fallback, won)` result. The term must not change.

| Case | Peers | Expected |
|---|---|---|
| single node wins | — | (false, true) |
| all peers grant | ✔ ✔ | (false, true) |
| all peers reject | ✘ ✘ | (false, false) |
| one of two grants (2/3) | ✔ ✘ | (false, true) |
| all peers unreachable | ⛔ ⛔ | (false, false) |
| exactly half is not a majority (2/4) | ✔ ✘ ✘ | (false, false) |
| bare majority of five (3/5) | ✔ ✔ ✘ ✘ | (false, true) |
| one grant plus unreachable (2/3) | ✔ ⛔ | (false, true) |

### `requestVotes`: other tests

| Test | Verifies |
|---|---|
| `SendsCorrectRequest` | Exactly one RPC per peer, with the term, the candidate's ID, and `LastLogIndex`/`LastLogTerm` taken from the log. |
| `DoesNotContactSelf` | A single-node call returns quickly. A self-RPC would block until the 2s RPC timeout. |
| `HigherTermFallsBack` | Returns `(true, false)`, adopts the higher term, and clears `votedFor`. |

### `handleRequestVote` (candidate): `TestCandidateHandleRequestVote` (table)

The candidate is at term 3, has voted for itself, and its log ends at index 1,
term 3.

| Case | Request | Expected |
|---|---|---|
| lower term | term 2 | reject, no fallback, term 3, `votedFor` self |
| same-term rival | term 3 | reject, no fallback, `votedFor` self |
| higher term, up-to-date log | term 4, lastLogTerm 3, lastIdx 1 | grant, fallback, term 4, `votedFor` = c |
| higher term, stale log term | term 4, lastLogTerm 1 | reject, fallback, term 4, `votedFor` cleared |
| higher term, shorter log, same term | term 4, lastLogTerm 3, lastIdx 0 | reject, fallback, term 4, `votedFor` cleared |

---

## `randomTimeout` tests (`util_test.go`)

| Test | Verifies |
|---|---|
| `NotNil` | Returns a real channel. A nil channel never fires, so a follower would never start an election. |
| `FiresWithinBounds` | Over 10 samples, every timeout fires no earlier than `min` and no later than `2*min` + slack. |
| `IsRandomized` | Ten 50ms timeouts are spread over at least 3ms. Fixed timeouts would cause endless split votes. |
| `IndependentChannels` | Each call returns its own channel, and both fire. |

## Fixes to the original test file

The original `node_follower_state_test.go` named its test
`testFollowerRequestVote` (lowercase), so `go test` never ran it. It also used
`defer n.Exit()`, which calls `os.Exit(0)` and would have ended the test
binary early while reporting success. Both problems are fixed. The table it
contained is kept, with more cases, in `TestFollowerRequestVote`.
