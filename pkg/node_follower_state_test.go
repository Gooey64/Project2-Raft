package pkg

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// followerHandleRequestVote
// ---------------------------------------------------------------------------

func vote(n *Node, req *RequestVoteRequest) (RequestVoteReply, bool, bool) {
	reply := make(chan RequestVoteReply, 1)
	reset, fallback := n.followerHandleRequestVote(RequestVoteMsg{req, reply})
	return <-reply, reset, fallback
}

func TestFollowerRequestVote(t *testing.T) {
	cand := &RemoteNode{Id: "cand", Addr: "x"}

	tests := []struct {
		name         string
		setup        func(n *Node)
		req          *RequestVoteRequest
		wantGrant    bool
		wantFallback bool
		wantTerm     uint64
		wantVotedFor string
	}{
		{
			name:         "lower term rejected",
			setup:        func(n *Node) { n.SetCurrentTerm(5) },
			req:          &RequestVoteRequest{Term: 4, Candidate: cand},
			wantGrant:    false,
			wantFallback: false,
			wantTerm:     5,
			wantVotedFor: "",
		},
		{
			name:         "same term, not yet voted, grants",
			setup:        func(n *Node) { n.SetCurrentTerm(2) },
			req:          &RequestVoteRequest{Term: 2, Candidate: cand},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "cand",
		},
		{
			name:         "already voted for someone else this term",
			setup:        func(n *Node) { n.SetCurrentTerm(2); n.setVotedFor("other") },
			req:          &RequestVoteRequest{Term: 2, Candidate: cand},
			wantGrant:    false,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "other",
		},
		{
			name:         "already voted for same candidate grants again",
			setup:        func(n *Node) { n.SetCurrentTerm(2); n.setVotedFor("cand") },
			req:          &RequestVoteRequest{Term: 2, Candidate: cand},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "cand",
		},
		{
			name:         "higher term clears previous vote and grants",
			setup:        func(n *Node) { n.SetCurrentTerm(2); n.setVotedFor("other") },
			req:          &RequestVoteRequest{Term: 3, Candidate: cand},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     3,
			wantVotedFor: "cand",
		},
		{
			name: "higher term but candidate last log term is older, rejected",
			setup: func(n *Node) {
				n.SetCurrentTerm(2)
				storeLogs(n, 1, 2)
				n.setVotedFor("other")
			},
			req:          &RequestVoteRequest{Term: 5, Candidate: cand, LastLogIndex: 5, LastLogTerm: 1},
			wantGrant:    false,
			wantFallback: true,
			wantTerm:     5,
			wantVotedFor: "", // cleared by the term bump, never set
		},
		{
			name: "higher last-log term wins despite shorter log",
			setup: func(n *Node) {
				n.SetCurrentTerm(1)
				storeLogs(n, 1, 1)
			},
			req:          &RequestVoteRequest{Term: 3, Candidate: cand, LastLogIndex: 1, LastLogTerm: 3},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     3,
			wantVotedFor: "cand",
		},
		{
			name: "same last term, shorter log rejected",
			setup: func(n *Node) {
				n.SetCurrentTerm(1)
				storeLogs(n, 1, 1)
			},
			req:          &RequestVoteRequest{Term: 2, Candidate: cand, LastLogIndex: 1, LastLogTerm: 1},
			wantGrant:    false,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "",
		},
		{
			name: "same last term, equal log length grants",
			setup: func(n *Node) {
				n.SetCurrentTerm(1)
				storeLogs(n, 1, 1)
			},
			req:          &RequestVoteRequest{Term: 2, Candidate: cand, LastLogIndex: 2, LastLogTerm: 1},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "cand",
		},
		{
			name: "same last term, longer log grants",
			setup: func(n *Node) {
				n.SetCurrentTerm(1)
				storeLogs(n, 1, 1)
			},
			req:          &RequestVoteRequest{Term: 2, Candidate: cand, LastLogIndex: 7, LastLogTerm: 1},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     2,
			wantVotedFor: "cand",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(t)
			tc.setup(n)

			reply, reset, fallback := vote(n, tc.req)
			if reply.VoteGranted != tc.wantGrant {
				t.Fatalf("VoteGranted = %v, want %v", reply.VoteGranted, tc.wantGrant)
			}
			if reset != tc.wantGrant {
				t.Errorf("resetTimeout = %v, want %v (only reset when granting)", reset, tc.wantGrant)
			}
			if fallback != tc.wantFallback {
				t.Errorf("fallback = %v, want %v", fallback, tc.wantFallback)
			}
			if reply.Term != tc.wantTerm {
				t.Errorf("reply.Term = %v, want %v", reply.Term, tc.wantTerm)
			}
			if got := n.GetCurrentTerm(); got != tc.wantTerm {
				t.Errorf("currentTerm = %v, want %v", got, tc.wantTerm)
			}
			if got := n.GetVotedFor(); got != tc.wantVotedFor {
				t.Errorf("votedFor = %q, want %q", got, tc.wantVotedFor)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleAppendEntries
// ---------------------------------------------------------------------------

func appendEntries(n *Node, req *AppendEntriesRequest) (AppendEntriesReply, bool, bool) {
	reply := make(chan AppendEntriesReply, 1)
	reset, fallback := n.handleAppendEntries(AppendEntriesMsg{req, reply})
	return <-reply, reset, fallback
}

func TestHandleAppendEntries_LowerTermRejected(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(5)
	before := n.lastHeardFromLeader.Load()

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, reset, fallback := appendEntries(n, &AppendEntriesRequest{Term: 4, Leader: leader})

	if reply.Success || reply.Term != 5 {
		t.Fatalf("reply = {Term:%v Success:%v}, want {Term:5 Success:false}", reply.Term, reply.Success)
	}
	if reset || fallback {
		t.Errorf("reset=%v fallback=%v, want both false", reset, fallback)
	}
	if n.getLeader() != nil {
		t.Errorf("leader should not be set by a stale request")
	}
	if n.lastHeardFromLeader.Load() != before {
		t.Errorf("lastHeardFromLeader should not change on stale request")
	}
}

func TestHandleAppendEntries_HeartbeatSetsLeaderAndTerm(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	n.setVotedFor("someone")

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, reset, fallback := appendEntries(n, &AppendEntriesRequest{
		Term: 3, Leader: leader, PrevLogIndex: 0, PrevLogTerm: 0,
	})

	if !reply.Success || reply.Term != 3 {
		t.Fatalf("reply = {Term:%v Success:%v}, want {Term:3 Success:true}", reply.Term, reply.Success)
	}
	if !reset || !fallback {
		t.Errorf("reset=%v fallback=%v, want both true", reset, fallback)
	}
	if got := n.getLeader(); got == nil || got.Id != "L" {
		t.Errorf("leader = %v, want L", got)
	}
	if n.GetCurrentTerm() != 3 {
		t.Errorf("term = %v, want 3", n.GetCurrentTerm())
	}
	if n.GetVotedFor() != "" {
		t.Errorf("votedFor = %q, want cleared", n.GetVotedFor())
	}
	if n.lastHeardFromLeader.Load() == 0 {
		t.Errorf("lastHeardFromLeader not updated")
	}
}

func TestHandleAppendEntries_SameTermKeepsVote(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(3)
	n.setVotedFor("L")

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{Term: 3, Leader: leader})
	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.GetVotedFor() != "L" {
		t.Errorf("votedFor = %q, should be unchanged for same-term request", n.GetVotedFor())
	}
}

func TestHandleAppendEntries_LeaderChange(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)

	old := &RemoteNode{Id: "old", Addr: "o"}
	n.setLeader(old)

	// Same leader: no change.
	appendEntries(n, &AppendEntriesRequest{Term: 1, Leader: old})
	if n.getLeader() != old {
		t.Errorf("leader changed even though the same leader sent the request")
	}

	// Different leader: replaced.
	newLeader := &RemoteNode{Id: "new", Addr: "n"}
	appendEntries(n, &AppendEntriesRequest{Term: 2, Leader: newLeader})
	if got := n.getLeader(); got == nil || got.Id != "new" {
		t.Errorf("leader = %v, want new", got)
	}
}

func TestHandleAppendEntries_PrevLogIndexTooHigh(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, reset, fallback := appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 5, PrevLogTerm: 1,
		Entries: []*LogEntry{{Index: 6, TermId: 1}},
	})

	if reply.Success {
		t.Fatal("expected rejection when PrevLogIndex > LastLogIndex")
	}
	// The leader is still valid; follower should reset and fall back.
	if !reset || !fallback {
		t.Errorf("reset=%v fallback=%v, want both true", reset, fallback)
	}
	if n.LastLogIndex() != 1 {
		t.Errorf("log modified on rejection: LastLogIndex = %v", n.LastLogIndex())
	}
}

func TestHandleAppendEntries_PrevLogTermMismatch(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(2)
	storeLogs(n, 1, 1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, reset, fallback := appendEntries(n, &AppendEntriesRequest{
		Term: 2, Leader: leader, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []*LogEntry{{Index: 3, TermId: 2}},
	})

	if reply.Success {
		t.Fatal("expected rejection when PrevLogTerm does not match")
	}
	if !reset || !fallback {
		t.Errorf("reset=%v fallback=%v, want both true", reset, fallback)
	}
	if n.LastLogIndex() != 2 {
		t.Errorf("log modified on rejection: LastLogIndex = %v", n.LastLogIndex())
	}
}

func TestHandleAppendEntries_AppendsNewEntries(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*LogEntry{
			{Index: 2, TermId: 1, Type: CommandType_NOOP},
			{Index: 3, TermId: 1, Type: CommandType_NOOP},
		},
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.LastLogIndex() != 3 {
		t.Fatalf("LastLogIndex = %v, want 3", n.LastLogIndex())
	}
	if n.CommitIndex.Load() != 0 {
		t.Errorf("CommitIndex = %v, want 0 (LeaderCommit was 0)", n.CommitIndex.Load())
	}
}

func TestHandleAppendEntries_TruncatesConflictingEntries(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(2)
	storeLogs(n, 1, 1, 1) // indices 1..3, all term 1

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{
		Term: 2, Leader: leader, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []*LogEntry{{Index: 2, TermId: 2, Type: CommandType_NOOP}},
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.LastLogIndex() != 2 {
		t.Fatalf("LastLogIndex = %v, want 2 (index 3 truncated)", n.LastLogIndex())
	}
	if e := n.GetLog(2); e == nil || e.TermId != 2 {
		t.Errorf("entry 2 = %v, want term 2 from leader", e)
	}
	if n.GetLog(3) != nil {
		t.Errorf("entry 3 should have been truncated")
	}
}

func TestHandleAppendEntries_PanicsOnLogGap(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	expectPanic(t, func() {
		reply := make(chan AppendEntriesReply, 1)
		n.handleAppendEntries(AppendEntriesMsg{&AppendEntriesRequest{
			Term: 1, Leader: leader, PrevLogIndex: 1, PrevLogTerm: 1,
			// First new entry does not follow our last index.
			Entries: []*LogEntry{{Index: 5, TermId: 1}},
		}, reply})
	})
}

func TestHandleAppendEntries_AdvancesCommitIndex(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*LogEntry{
			{Index: 1, TermId: 1, Type: CommandType_NOOP},
			{Index: 2, TermId: 1, Type: CommandType_NOOP},
			{Index: 3, TermId: 1, Type: CommandType_NOOP},
		},
		LeaderCommit: 2,
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.CommitIndex.Load() != 2 {
		t.Errorf("CommitIndex = %v, want 2", n.CommitIndex.Load())
	}
	if n.LastApplied.Load() != 2 {
		t.Errorf("LastApplied = %v, want 2", n.LastApplied.Load())
	}
}

func TestHandleAppendEntries_CommitIndexCappedAtLastLogIndex(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1, 1)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 10,
	})

	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.CommitIndex.Load() != 2 {
		t.Errorf("CommitIndex = %v, want min(10, 2) = 2", n.CommitIndex.Load())
	}
	if n.LastApplied.Load() != 2 {
		t.Errorf("LastApplied = %v, want 2", n.LastApplied.Load())
	}
}

func TestHandleAppendEntries_AppliesCommittedCommands(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	sm := n.StateMachine.(*fakeStateMachine)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []*LogEntry{
			{Index: 1, TermId: 1, Type: CommandType_CLIENT_REGISTRATION, CacheId: CreateCacheID(0, 1)},
			{Index: 2, TermId: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: CreateCacheID(1, 1), Data: []byte("x")},
		},
		LeaderCommit: 2,
	})

	if got := len(sm.GetState().([][]byte)); got != 1 {
		t.Errorf("state machine applied %d commands, want 1", got)
	}
	if _, ok := n.GetCachedReply(CreateCacheID(1, 1)); !ok {
		t.Errorf("follower should cache the reply for an applied command")
	}
}

func TestHandleAppendEntries_LeaderCommitNotAhead(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1, 1)
	n.CommitIndex.Store(2)
	n.LastApplied.Store(2)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	reply, _, _ := appendEntries(n, &AppendEntriesRequest{
		Term: 1, Leader: leader, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 1,
	})
	if !reply.Success {
		t.Fatal("expected success")
	}
	if n.CommitIndex.Load() != 2 {
		t.Errorf("CommitIndex = %v, must never move backwards", n.CommitIndex.Load())
	}
}

// ---------------------------------------------------------------------------
// doFollower (state loop)
// ---------------------------------------------------------------------------

// newFollower returns a node with a long election timeout so the loop does not
// time out into the candidate state in tests that are not about timeouts.
func newFollower(t *testing.T) *Node {
	n := newTestNode(t)
	n.Config.ElectionTimeout = time.Hour
	return n
}

func TestDoFollower_SetsStateAndExitsOnShutdown(t *testing.T) {
	n := newFollower(t)
	next := runState(n.doFollower)

	// A false shutdown signal must be ignored.
	sendExit(t, n, false)
	assertStillRunning(t, next)
	if n.GetState() != FollowerState {
		t.Errorf("state = %v, want FollowerState", n.GetState())
	}

	sendExit(t, n, true)
	if f := waitNext(t, next); f != nil {
		t.Errorf("doFollower should return nil on graceful exit")
	}
}

func TestDoFollower_HandlesAppendEntries(t *testing.T) {
	n := newFollower(t)
	n.SetCurrentTerm(2)
	next := runState(n.doFollower)
	defer func() { sendExit(t, n, true); waitNext(t, next) }()

	leader := &RemoteNode{Id: "L", Addr: "l"}

	// Accepted request (resets the timeout).
	r := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 2, Leader: leader}))
	if !r.Success {
		t.Errorf("valid AppendEntries rejected")
	}
	// Stale request (does not reset the timeout).
	r = waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 1, Leader: leader}))
	if r.Success {
		t.Errorf("stale AppendEntries accepted")
	}
	assertStillRunning(t, next)
}

func TestDoFollower_HandlesRequestVote(t *testing.T) {
	n := newFollower(t)
	n.SetCurrentTerm(2)
	next := runState(n.doFollower)
	defer func() { sendExit(t, n, true); waitNext(t, next) }()

	// Granted vote (resets the timeout).
	r := waitRVReply(t, sendRV(t, n, &RequestVoteRequest{Term: 3, Candidate: &RemoteNode{Id: "A"}}))
	if !r.VoteGranted {
		t.Errorf("expected vote for A")
	}
	// Rejected vote: already voted for A this term.
	r = waitRVReply(t, sendRV(t, n, &RequestVoteRequest{Term: 3, Candidate: &RemoteNode{Id: "B"}}))
	if r.VoteGranted {
		t.Errorf("voted twice in the same term")
	}
	assertStillRunning(t, next)
}

func TestDoFollower_RejectsClientRequests(t *testing.T) {
	n := newFollower(t)
	leader := &RemoteNode{Id: "L", Addr: "l"}
	n.setLeader(leader)
	next := runState(n.doFollower)
	defer func() { sendExit(t, n, true); waitNext(t, next) }()

	r := waitClientReply(t, sendClient(t, n, &ClientRequest{ClientId: 1, SequenceNum: 1}))
	if r.Status != ClientStatus_NOT_LEADER {
		t.Errorf("status = %v, want NOT_LEADER", r.Status)
	}
	if r.LeaderHint == nil || r.LeaderHint.Id != "L" {
		t.Errorf("LeaderHint = %v, want L", r.LeaderHint)
	}
}

func TestDoFollower_ElectionTimeoutBecomesCandidate(t *testing.T) {
	n := newTestNode(t)
	n.Config.ElectionTimeout = 10 * time.Millisecond
	next := waitNext(t, runState(n.doFollower))
	if !sameState(next, n.doCandidate) {
		t.Errorf("follower should transition to candidate on election timeout")
	}
}

// Timing for the timer-reset tests: the follower times out within
// [resetTimeout, 2*resetTimeout] of its last reset, and messages arrive every
// resetInterval, leaving a wide margin for scheduling delays. Messages keep
// coming for resetDuration, well past 2*resetTimeout.
const (
	resetTimeout  = 150 * time.Millisecond
	resetInterval = 20 * time.Millisecond
	resetDuration = 700 * time.Millisecond
)

// keepSending calls send every resetInterval for resetDuration, failing if the
// follower leaves the follower state meanwhile. send delivers one message and
// returns false if the follower returned before it could be delivered.
func keepSending(t *testing.T, next <-chan stateFunction, send func(next <-chan stateFunction) bool) {
	t.Helper()
	stop := time.After(resetDuration)
	tick := time.NewTicker(resetInterval)
	defer tick.Stop()
	for {
		select {
		case <-next:
			t.Fatal("follower left the follower state while its timer kept being reset")
		case <-stop:
			return
		case <-tick.C:
			if !send(next) {
				t.Fatal("follower left the follower state while its timer kept being reset")
			}
		}
	}
}

func TestDoFollower_HeartbeatsResetTimeout(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	n.Config.ElectionTimeout = resetTimeout
	leader := &RemoteNode{Id: "L", Addr: "l"}
	next := runState(n.doFollower)

	keepSending(t, next, func(next <-chan stateFunction) bool {
		reply := make(chan AppendEntriesReply, 1)
		select {
		case n.appendEntries <- AppendEntriesMsg{&AppendEntriesRequest{Term: 1, Leader: leader}, reply}:
			return waitAEReply(t, reply).Success
		case <-next:
			return false
		}
	})

	// Once heartbeats stop, the follower must time out.
	if f := waitNext(t, next); !sameState(f, n.doCandidate) {
		t.Error("follower should become candidate after heartbeats stop")
	}
}

func TestDoFollower_GrantedVoteResetsTimeout(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	n.Config.ElectionTimeout = resetTimeout
	next := runState(n.doFollower)

	// A fresh term each time so every vote is granted.
	term := uint64(1)
	keepSending(t, next, func(next <-chan stateFunction) bool {
		term++
		reply := make(chan RequestVoteReply, 1)
		req := &RequestVoteRequest{Term: term, Candidate: &RemoteNode{Id: "c"}}
		select {
		case n.requestVote <- RequestVoteMsg{req, reply}:
			if !waitRVReply(t, reply).VoteGranted {
				t.Errorf("vote for term %d not granted", term)
			}
			return true
		case <-next:
			return false
		}
	})

	if f := waitNext(t, next); !sameState(f, n.doCandidate) {
		t.Error("follower should become candidate once votes stop")
	}
}

func TestDoFollower_StaleMessagesDoNotResetTimeout(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(5)
	n.setVotedFor("someone")
	n.Config.ElectionTimeout = 60 * time.Millisecond // must fire by ~120ms
	next := runState(n.doFollower)

	deadline := time.After(waitTimeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	for {
		select {
		case f := <-next:
			if !sameState(f, n.doCandidate) {
				t.Fatal("follower should become candidate")
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("timed out after %v; stale messages appear to reset the timer", d)
			}
			return
		case <-tick.C:
			// Rejected AppendEntries (old term), rejected vote (already voted),
			// and client requests must not keep the follower alive. Sends are
			// non-blocking because the follower may already have timed out.
			select {
			case n.appendEntries <- AppendEntriesMsg{&AppendEntriesRequest{Term: 1, Leader: &RemoteNode{Id: "old"}}, make(chan AppendEntriesReply, 1)}:
			default:
			}
			select {
			case n.requestVote <- RequestVoteMsg{&RequestVoteRequest{Term: 5, Candidate: &RemoteNode{Id: "c"}}, make(chan RequestVoteReply, 1)}:
			default:
			}
			select {
			case n.clientRequest <- ClientRequestMsg{&ClientRequest{ClientId: 1}, make(chan ClientReply, 1)}:
			default:
			}
		case <-deadline:
			t.Fatal("follower never timed out while receiving only stale messages")
		}
	}
}
