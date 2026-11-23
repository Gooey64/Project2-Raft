# About
randomTimeout, doCandidate, requestVotes, and handleRequestVote were completed by Jeremiah.
doFollower, doLeader, and sendHeartbeats were completed by Rebecca.
Simple preliminary tests for each function were devised individually. Most test cases were then devised collaboratively.

## Files
Our unit tests are split into different files. Those files are:
- `util_test.go`
- `node_follower_state_test.go`
- `node_candidate_state_test.go`
- `node_leader_state_test.go`

There is also an additional file (`node_test_helpers_test.go`) that contains helper functions for tests, but no actual tests are contained in this file. These helper functions do the following:
- `newTestNode` creates a new node with a fake state machine
- `fakeStateMachine` records commands
- `fakePeer` creates a node for test leaders or candidates to talk to over the `appendEntries` channel
- `fakeVoter` creates a node for test leaders or candidates to talk to over the `requestVote` channel
- `voteRecorder` records requests it receives and replies with a decision
- `deniedPeer` creates an unreachable peer
- `runState` runs a state function
- `waitNext` waits for the next state
- `assertStillRunning` checks that a state is still running
- `sameState` compares two state functions
- `sendAE` delivers a message to a running state function
- `sendRV` delivers a message to a running state function
- `sendClient` delivers a message to a running state function
- `sendExit` delivers a message to a running state function
- `eventually` polls a condition until it is true or times out
- `expectPanic` tests that a function will panic (fails if it does not)
- `storeLogs` appends entries

## `util_test.go`
`randomTimeout` tests
- `NotNil` tests that randomTimeout returns a real channel
- `FiresWithinBounds` tests that every timeout fires no earlier than `min` and no later than `2*min + slack`
- `IsRandomized` tests that 50ms timeouts are spread over at least 3ms
- `NonPositiveFiresImmediately` tests that a negative minimum doesn't panic and fire right away
- `randomTimeout(0)` tests that a 0 minimum doesn't panic and fire right away
- `IndependentChannels` tests that each call returns its own channel and that both can fire.

## `node_follower_state_test.go`
`followerHandleRequestVote` tests (`TestFollowerRequestVote`, table-driven). Every case checks the vote grant, `resetTimeout` (true only when the vote is granted), `fallback`, the reply term, the stored term, and `votedFor`.
- `lower term rejected` tests that a request with term 4 sent to a node at term 5 is rejected, with no fallback, and the term stays 5
- `same term, not yet voted, grants` tests that a same-term request is granted when the node has not voted yet, and `votedFor` is set to the candidate
- `already voted for someone else this term` tests that a same-term request is rejected when the node already voted for a different candidate, and `votedFor` is unchanged
- `already voted for same candidate grants again` tests that a repeat request from the candidate the node already voted for is granted again (the vote is idempotent)
- `higher term clears previous vote and grants` tests that a higher-term request updates the term, clears the previous vote, and grants the new vote
- `higher term but candidate last log term is older, rejected` tests that a higher-term request with an older last-log term is rejected, but the term is still updated and `votedFor` is cleared
- `higher last-log term wins despite shorter log` tests that a candidate with a newer last-log term is granted the vote even though its log is shorter
- `same last term, shorter log rejected` tests that a candidate with the same last-log term but a shorter log is rejected
- `same last term, equal log length grants` tests that a candidate with the same last-log term and an equal log length is granted the vote
- `same last term, longer log grants` tests that a candidate with the same last-log term and a longer log is granted the vote

`handleAppendEntries` tests
- `LowerTermRejected` tests that a stale term is rejected with our term, and does not reset the timer, fall back, set the leader, or touch `lastHeardFromLeader`
- `HeartbeatSetsLeaderAndTerm` tests that a higher-term heartbeat succeeds, sets reset and fallback, records the leader, adopts the term, clears `votedFor`, and updates `lastHeardFromLeader`
- `SameTermKeepsVote` tests that a same-term request does not clear `votedFor`
- `LeaderChange` tests that the leader is unchanged when the same leader sends again, and replaced when a different leader sends
- `PrevLogIndexTooHigh` tests that a request is rejected when `PrevLogIndex > LastLogIndex`, the log is untouched, and reset and fallback are still true (the leader is valid)
- `PrevLogTermMismatch` tests that a request is rejected when the entry at `PrevLogIndex` has a different term, and the log is untouched
- `AppendsNewEntries` tests that new entries are appended and the commit index is unchanged when `LeaderCommit = 0`
- `TruncatesConflictingEntries` tests that a conflicting suffix is removed and replaced with the leader's entry
- `PanicsOnLogGap` tests that entries that do not start at `LastLogIndex+1` trigger the sanity-check panic
- `AdvancesCommitIndex` tests that `CommitIndex` and `LastApplied` advance to `LeaderCommit`
- `CommitIndexCappedAtLastLogIndex` tests that the commit index becomes `min(LeaderCommit, LastLogIndex)`
- `AppliesCommittedCommands` tests that committed registration and command entries reach the state machine and their replies are cached
- `LeaderCommitNotAhead` tests that the commit index never moves backwards

`doFollower` loop tests (tests that are not about timing use a one-hour election timeout so they never drift into the candidate state)
- `SetsStateAndExitsOnShutdown` tests that the state becomes `FollowerState`, `gracefulExit <- false` is ignored, and `true` returns `nil`
- `HandlesAppendEntries` tests that valid and stale AppendEntries get correct replies and the loop keeps running
- `HandlesRequestVote` tests that the first vote of a term is granted, a second candidate in the same term is refused, and the loop keeps running
- `RejectsClientRequests` tests that clients get `NOT_LEADER` with the known leader as the hint
- `ElectionTimeoutBecomesCandidate` tests that with no messages, the follower returns `n.doCandidate`
- `HeartbeatsResetTimeout` tests that heartbeats every 20ms for 700ms (timeout 150–300ms) keep the node a follower, and that it becomes a candidate once they stop
- `GrantedVoteResetsTimeout` tests the same as `HeartbeatsResetTimeout`, but using granted votes (a new term each time) instead of heartbeats
- `StaleMessagesDoNotResetTimeout` tests that stale AppendEntries, refused votes, and client requests do not keep the follower alive, and it still times out

## `node_leader_state_test.go`
`leaderHandleClientRequest` tests
- `Registration` tests that `ClientId 0` appends a `CLIENT_REGISTRATION` entry at the next index with the current term and cache ID `0-<index>`, updates `matchIndex[self]`, and registers the reply channel without answering it yet
- `NewCommand` tests that a `STATE_MACHINE_COMMAND` entry carries the command, data, and cache ID `<client>-<seq>`, and the reply is registered
- `CachedReply` tests that an already-applied request gets its cached reply immediately and is not appended
- `DuplicatePending` tests that a retry of an in-flight request adds a second waiter but no second log entry

`stepDownFromLeader` tests
- `StepDownFromLeader` tests that every waiting client (several per cache ID, several IDs) gets the given status plus a leader hint, and the pending map is emptied
- `NoPending` tests that it does not block or panic when nothing is pending

`sendHeartbeats` tests
- `SingleNodeCommitsImmediately` tests that a cluster of one commits and applies current-term entries right away
- `DoesNotCommitOldTermEntries` tests that entries from an older term are not committed by counting replicas (Raft §5.4.2)
- `NoMajorityNoCommit` tests that with unreachable peers there is no commit and no fallback, and `nextIndex` is unchanged
- `SuccessReplicatesAndCommits` tests that the request has the right `PrevLogIndex`/`PrevLogTerm`, entries, leader, and term, that `matchIndex`/`nextIndex` advance on success, and that the entry commits once 2 of 3 nodes have it
- `FailureDecrementsNextIndex` tests that a rejection decrements `nextIndex`, which never goes below 1
- `HigherTermFallsBack` tests that a higher-term reply returns `true`, adopts the term, clears `votedFor`, and commits nothing

`processLogEntry` tests
- `Noop` tests that a NOOP entry does nothing and returns `false`
- `Registration` tests that the client gets OK with `ClientId = log index` and a self leader hint, the reply is cached, and the pending entry is removed
- `CommandSuccess` tests that the state machine is applied and every waiter gets OK, the client ID, and the response
- `CommandFailure` tests that a state machine error becomes `REQ_FAILED` with the error text
- `NoWaitingClient` tests that the reply is cached even when no one is waiting (the follower path)
- `InvalidCacheIDPanics` tests that a non-numeric client ID in the cache ID panics
- `AlreadyCachedPanics` tests that a command whose reply is already cached panics.

`doLeader` loop tests
- `SingleNodeLifecycle` tests that the leader appends a NOOP for its term and sets itself as leader and state, that registration and commands are committed on the heartbeat tick and answered, that stale and same-term votes and stale AppendEntries are refused without stepping down, and that `gracefulExit false` is ignored while `true` returns `nil`
- `InitializesPeerIndices` tests that `nextIndex = lastIndex+1` and `matchIndex = 0` for each peer, and `matchIndex[self]` points at the NOOP
- `StepsDownOnHigherTermAppendEntries` tests that a higher-term AppendEntries returns `doFollower`, waiting clients get `NOT_LEADER` with the new leader's hint, and the term is updated
- `StepsDownOnHigherTermRequestVote` tests that a higher-term RequestVote is granted, the leader returns `doFollower`, and waiting clients get `ELECTION_IN_PROGRESS`
- `InitialHeartbeatFallback` tests that a higher term on the very first heartbeat causes an immediate step-down and pending clients are notified
- `TickerHeartbeatFallback` tests that the leader succeeds for a few periodic heartbeats (so the NOOP commits), then steps down when a later heartbeat sees a higher term

## `node_candidate_state_test.go`
`doCandidate` election tests (most loop tests use `stuckCandidate`: a 3-node cluster with two unreachable peers and a one-hour timeout, so the candidate sits in its election loop waiting for votes that never arrive)
- `StartsElection` tests that the state becomes `CandidateState`, the term goes up by one, and the node votes for itself
- `SingleNodeWins` tests that a cluster of one returns `n.doLeader` at the new term
- `WinsWithMajority` tests that with all votes the candidate returns `doLeader`, and each peer got a RequestVote with the new term, the candidate's ID, and the correct `LastLogIndex`/`LastLogTerm`
- `WinsDespiteUnreachablePeer` tests that 2 of 3 votes (one peer unreachable) is enough to win
- `HigherTermVoteReplyFallsBack` tests that a vote reply with a higher term returns `doFollower` and adopts the term
- `LostElectionRetriesWithHigherTerm` tests that when all peers refuse, the candidate times out, starts another election at term +1 (peers see the new term), and stays a candidate

`doCandidate` tests for messages received while waiting for votes
- `AppendEntriesFromNewLeaderFallsBack` tests that a same-term leader's AppendEntries is accepted, the leader is recorded, and the candidate returns `doFollower`
- `HigherTermAppendEntriesFallsBack` tests that a higher-term AppendEntries is accepted and the candidate returns `doFollower`
- `StaleAppendEntriesRejected` tests that a lower-term AppendEntries is refused and the node stays a candidate
- `HigherTermRequestVoteFallsBack` tests that the vote is granted to a higher-term candidate, the term and `votedFor` are updated, and the candidate returns `doFollower`
- `RivalSameTermRequestVoteRejected` tests that a rival in the same term is refused (the candidate already voted for itself) and the node stays a candidate
- `ClientRequestRejected` tests that clients get `NOT_LEADER` or `ELECTION_IN_PROGRESS` and the node stays a candidate
- `GracefulExit` tests that `false` is ignored and `true` returns `nil`

`requestVotes` tests (`TestRequestVotes`, table-driven). Each case starts with the node already a candidate at term 2 that has voted for itself, checks the `(fallback, won)` result, and checks that the term does not change.
- `single node wins` tests that a cluster of one returns `(false, true)`
- `all peers grant` tests that two granting peers return `(false, true)`
- `all peers reject` tests that two rejecting peers return `(false, false)`
- `one of two peers grants (2/3)` tests that one grant and one rejection return `(false, true)`
- `all peers unreachable` tests that two unreachable peers return `(false, false)`
- `exactly half is not a majority (2/4)` tests that one grant and two rejections in a 4-node cluster return `(false, false)`
- `bare majority of five (3/5)` tests that two grants and two rejections in a 5-node cluster return `(false, true)`
- `one grant plus unreachable (2/3)` tests that one grant and one unreachable peer return `(false, true)`

`requestVotes` other tests
- `SendsCorrectRequest` tests that exactly one RPC is sent per peer, with the term, the candidate's ID, and `LastLogIndex`/`LastLogTerm` taken from the log
- `DoesNotContactSelf` tests that a single-node call returns quickly (a self-RPC would block until the 2s RPC timeout)
- `HigherTermFallsBack` tests that a higher-term reply returns `(true, false)`, adopts the higher term, and clears `votedFor`

`handleRequestVote` (candidate) tests (`TestCandidateHandleRequestVote`, table-driven). The candidate is at term 3, has voted for itself, and its log ends at index 1, term 3.
- `lower term rejected, stay candidate` tests that a term 2 request is rejected with no fallback, the term stays 3, and `votedFor` stays self
- `same term rival rejected, stay candidate` tests that a term 3 request is rejected with no fallback and `votedFor` stays self
- `higher term with up-to-date log: grant and fall back` tests that a term 4 request with last-log term 3 and last index 1 is granted, falls back, sets the term to 4, and sets `votedFor` to the candidate
- `higher term with stale log: reject but still fall back` tests that a term 4 request with last-log term 1 is rejected, but still falls back, sets the term to 4, and clears `votedFor`
- `higher term, same last term but shorter log: reject but fall back` tests that a term 4 request with last-log term 3 and last index 0 is rejected, but still falls back, sets the term to 4, and clears `votedFor`
