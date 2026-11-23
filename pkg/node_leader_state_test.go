package pkg

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
)

// ---------------------------------------------------------------------------
// leaderHandleClientRequest
// ---------------------------------------------------------------------------

func TestLeaderHandleClientRequest_Registration(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(4)
	storeLogs(n, 4)

	reply := make(chan ClientReply, 1)
	n.leaderHandleClientRequest(ClientRequestMsg{&ClientRequest{ClientId: 0}, reply})

	e := n.GetLog(2)
	if e == nil {
		t.Fatal("registration entry not appended at index 2")
	}
	if e.Type != CommandType_CLIENT_REGISTRATION || e.TermId != 4 || e.CacheId != CreateCacheID(0, 2) {
		t.Errorf("entry = %v, want CLIENT_REGISTRATION term 4 cacheId 0-2", e)
	}
	if n.matchIndex[n.Self.Id] != 2 {
		t.Errorf("matchIndex[self] = %v, want 2", n.matchIndex[n.Self.Id])
	}
	if got := len(n.requestsByCacheID[e.CacheId]); got != 1 {
		t.Errorf("pending replies for %s = %d, want 1", e.CacheId, got)
	}
	select {
	case <-reply:
		t.Error("client must not be answered before the entry commits")
	default:
	}
}

func TestLeaderHandleClientRequest_NewCommand(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)

	reply := make(chan ClientReply, 1)
	req := &ClientRequest{ClientId: 7, SequenceNum: 3, StateMachineCmd: 9, Data: []byte("d")}
	n.leaderHandleClientRequest(ClientRequestMsg{req, reply})

	e := n.GetLog(1)
	if e == nil || e.Type != CommandType_STATE_MACHINE_COMMAND {
		t.Fatalf("entry = %v, want STATE_MACHINE_COMMAND at index 1", e)
	}
	if e.Command != 9 || !bytes.Equal(e.Data, []byte("d")) || e.CacheId != "7-3" {
		t.Errorf("entry fields = {cmd:%v data:%q cache:%q}", e.Command, e.Data, e.CacheId)
	}
	if got := len(n.requestsByCacheID["7-3"]); got != 1 {
		t.Errorf("pending replies = %d, want 1", got)
	}
}

func TestLeaderHandleClientRequest_CachedReply(t *testing.T) {
	n := newTestNode(t)
	cached := ClientReply{Status: ClientStatus_OK, ClientId: 7, Response: []byte("cached")}
	if err := n.CacheClientReply("7-3", cached); err != nil {
		t.Fatal(err)
	}

	reply := make(chan ClientReply, 1)
	n.leaderHandleClientRequest(ClientRequestMsg{&ClientRequest{ClientId: 7, SequenceNum: 3}, reply})

	r := waitClientReply(t, reply)
	if !bytes.Equal(r.Response, []byte("cached")) {
		t.Errorf("response = %q, want cached", r.Response)
	}
	if n.LastLogIndex() != 0 {
		t.Errorf("a cached request must not be appended to the log")
	}
}

func TestLeaderHandleClientRequest_DuplicatePending(t *testing.T) {
	n := newTestNode(t)
	first := make(chan ClientReply, 1)
	second := make(chan ClientReply, 1)
	req := &ClientRequest{ClientId: 7, SequenceNum: 3}

	n.leaderHandleClientRequest(ClientRequestMsg{req, first})
	n.leaderHandleClientRequest(ClientRequestMsg{req, second})

	if n.LastLogIndex() != 1 {
		t.Errorf("LastLogIndex = %v, duplicate request must not be appended twice", n.LastLogIndex())
	}
	if got := len(n.requestsByCacheID["7-3"]); got != 2 {
		t.Errorf("pending replies = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// stepDownFromLeader
// ---------------------------------------------------------------------------

func TestStepDownFromLeader(t *testing.T) {
	n := newTestNode(t)
	leader := &RemoteNode{Id: "L", Addr: "l"}
	n.setLeader(leader)

	a1, a2, b := make(chan ClientReply, 1), make(chan ClientReply, 1), make(chan ClientReply, 1)
	n.requestsByCacheID["1-1"] = []chan ClientReply{a1, a2}
	n.requestsByCacheID["2-1"] = []chan ClientReply{b}

	n.stepDownFromLeader(ClientStatus_ELECTION_IN_PROGRESS)

	for i, ch := range []chan ClientReply{a1, a2, b} {
		r := waitClientReply(t, ch)
		if r.Status != ClientStatus_ELECTION_IN_PROGRESS {
			t.Errorf("reply %d status = %v, want ELECTION_IN_PROGRESS", i, r.Status)
		}
		if r.LeaderHint == nil || r.LeaderHint.Id != "L" {
			t.Errorf("reply %d LeaderHint = %v, want L", i, r.LeaderHint)
		}
	}
	if len(n.requestsByCacheID) != 0 {
		t.Errorf("pending map not cleared: %v", n.requestsByCacheID)
	}
}

func TestStepDownFromLeader_NoPending(t *testing.T) {
	n := newTestNode(t)
	n.stepDownFromLeader(ClientStatus_NOT_LEADER) // must not block or panic
}

// ---------------------------------------------------------------------------
// sendHeartbeats
// ---------------------------------------------------------------------------

func TestSendHeartbeats_SingleNodeCommitsImmediately(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(2)
	storeLogs(n, 2, 2)

	if n.sendHeartbeats() {
		t.Fatal("unexpected fallback")
	}
	if n.CommitIndex.Load() != 2 || n.LastApplied.Load() != 2 {
		t.Errorf("commit=%v applied=%v, want 2/2", n.CommitIndex.Load(), n.LastApplied.Load())
	}
}

func TestSendHeartbeats_DoesNotCommitOldTermEntries(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(3)
	storeLogs(n, 1, 2) // last entry from an older term

	if n.sendHeartbeats() {
		t.Fatal("unexpected fallback")
	}
	if n.CommitIndex.Load() != 0 {
		t.Errorf("CommitIndex = %v, leader must not commit entries from older terms by counting", n.CommitIndex.Load())
	}
}

func TestSendHeartbeats_NoMajorityNoCommit(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1)
	p1, p2 := deniedPeer(n, "p1"), deniedPeer(n, "p2")
	n.setPeers([]*RemoteNode{n.Self, p1, p2})
	n.nextIndex[p1.Id], n.nextIndex[p2.Id] = 1, 1

	if n.sendHeartbeats() {
		t.Fatal("unreachable peers must not cause fallback")
	}
	if n.CommitIndex.Load() != 0 {
		t.Errorf("CommitIndex = %v, want 0 without a majority", n.CommitIndex.Load())
	}
	if n.nextIndex[p1.Id] != 1 {
		t.Errorf("nextIndex changed for a peer whose RPC failed")
	}
}

func TestSendHeartbeats_SuccessReplicatesAndCommits(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1, 1)

	var mu sync.Mutex
	var got *AppendEntriesRequest
	p := fakePeer(t, func(req *AppendEntriesRequest) AppendEntriesReply {
		mu.Lock()
		got = req
		mu.Unlock()
		return AppendEntriesReply{Term: 1, Success: true}
	})
	dead := deniedPeer(n, "dead")
	n.setPeers([]*RemoteNode{n.Self, p.Self, dead})
	n.nextIndex[p.Self.Id], n.nextIndex[dead.Id] = 2, 1

	if n.sendHeartbeats() {
		t.Fatal("unexpected fallback")
	}

	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("peer received no AppendEntries")
	}
	if got.PrevLogIndex != 1 || got.PrevLogTerm != 1 || len(got.Entries) != 1 || got.Entries[0].Index != 2 {
		t.Errorf("request = {prev:%v prevTerm:%v entries:%v}, want prev 1, one entry at index 2",
			got.PrevLogIndex, got.PrevLogTerm, got.Entries)
	}
	if got.Leader.Id != n.Self.Id || got.Term != 1 {
		t.Errorf("request leader/term = %v/%v", got.Leader.Id, got.Term)
	}
	if n.matchIndex[p.Self.Id] != 2 || n.nextIndex[p.Self.Id] != 3 {
		t.Errorf("match/next = %v/%v, want 2/3", n.matchIndex[p.Self.Id], n.nextIndex[p.Self.Id])
	}
	if n.CommitIndex.Load() != 2 || n.LastApplied.Load() != 2 {
		t.Errorf("commit=%v applied=%v, want 2/2 (2 of 3 have it)", n.CommitIndex.Load(), n.LastApplied.Load())
	}
}

func TestSendHeartbeats_FailureDecrementsNextIndex(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	storeLogs(n, 1, 1)

	p := fakePeer(t, func(req *AppendEntriesRequest) AppendEntriesReply {
		return AppendEntriesReply{Term: 1, Success: false}
	})
	n.setPeers([]*RemoteNode{n.Self, p.Self})

	n.nextIndex[p.Self.Id] = 3
	if n.sendHeartbeats() {
		t.Fatal("unexpected fallback")
	}
	if n.nextIndex[p.Self.Id] != 2 {
		t.Errorf("nextIndex = %v, want 2", n.nextIndex[p.Self.Id])
	}

	// nextIndex never drops below 1.
	n.nextIndex[p.Self.Id] = 1
	n.sendHeartbeats()
	if n.nextIndex[p.Self.Id] != 1 {
		t.Errorf("nextIndex = %v, must not go below 1", n.nextIndex[p.Self.Id])
	}
	if n.CommitIndex.Load() != 0 {
		t.Errorf("CommitIndex = %v, want 0", n.CommitIndex.Load())
	}
}

func TestSendHeartbeats_HigherTermFallsBack(t *testing.T) {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	n.setVotedFor(n.Self.Id)
	storeLogs(n, 1)

	p := fakePeer(t, func(req *AppendEntriesRequest) AppendEntriesReply {
		return AppendEntriesReply{Term: 9, Success: false}
	})
	n.setPeers([]*RemoteNode{n.Self, p.Self})
	n.nextIndex[p.Self.Id] = 1

	if !n.sendHeartbeats() {
		t.Fatal("expected fallback on higher term reply")
	}
	if n.GetCurrentTerm() != 9 {
		t.Errorf("term = %v, want 9", n.GetCurrentTerm())
	}
	if n.GetVotedFor() != "" {
		t.Errorf("votedFor = %q, want cleared", n.GetVotedFor())
	}
	if n.CommitIndex.Load() != 0 {
		t.Errorf("must not commit after discovering a higher term")
	}
}

// ---------------------------------------------------------------------------
// processLogEntry
// ---------------------------------------------------------------------------

func TestProcessLogEntry_Noop(t *testing.T) {
	n := newTestNode(t)
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_NOOP})
	if n.processLogEntry(1) {
		t.Error("processLogEntry should never request fallback")
	}
}

func TestProcessLogEntry_Registration(t *testing.T) {
	n := newTestNode(t)
	cacheID := CreateCacheID(0, 5)
	n.StoreLog(&LogEntry{Index: 5, Type: CommandType_CLIENT_REGISTRATION, CacheId: cacheID})
	ch := make(chan ClientReply, 1)
	n.requestsByCacheID[cacheID] = []chan ClientReply{ch}

	n.processLogEntry(5)

	r := waitClientReply(t, ch)
	if r.Status != ClientStatus_OK || r.ClientId != 5 {
		t.Errorf("reply = {status:%v id:%v}, want OK / 5", r.Status, r.ClientId)
	}
	if r.LeaderHint == nil || r.LeaderHint.Id != n.Self.Id {
		t.Errorf("LeaderHint = %v, want self", r.LeaderHint)
	}
	if _, ok := n.requestsByCacheID[cacheID]; ok {
		t.Error("pending entry not removed")
	}
	if _, ok := n.GetCachedReply(cacheID); !ok {
		t.Error("reply not cached")
	}
}

func TestProcessLogEntry_CommandSuccess(t *testing.T) {
	n := newTestNode(t)
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: "7-1", Data: []byte("x")})
	ch1, ch2 := make(chan ClientReply, 1), make(chan ClientReply, 1)
	n.requestsByCacheID["7-1"] = []chan ClientReply{ch1, ch2}

	n.processLogEntry(1)

	for _, ch := range []chan ClientReply{ch1, ch2} {
		r := waitClientReply(t, ch)
		if r.Status != ClientStatus_OK || r.ClientId != 7 || !bytes.Equal(r.Response, []byte("ok:x")) {
			t.Errorf("reply = {status:%v id:%v resp:%q}", r.Status, r.ClientId, r.Response)
		}
	}
}

func TestProcessLogEntry_CommandFailure(t *testing.T) {
	n := newTestNode(t)
	n.StateMachine.(*fakeStateMachine).fail = true
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: "7-1"})
	ch := make(chan ClientReply, 1)
	n.requestsByCacheID["7-1"] = []chan ClientReply{ch}

	n.processLogEntry(1)

	r := waitClientReply(t, ch)
	if r.Status != ClientStatus_REQ_FAILED || string(r.Response) != "apply failed" {
		t.Errorf("reply = {status:%v resp:%q}, want REQ_FAILED / apply failed", r.Status, r.Response)
	}
}

func TestProcessLogEntry_NoWaitingClient(t *testing.T) {
	n := newTestNode(t)
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: "7-1"})
	n.processLogEntry(1) // follower path: nobody waiting, still cached
	if _, ok := n.GetCachedReply("7-1"); !ok {
		t.Error("reply not cached")
	}
}

func TestProcessLogEntry_InvalidCacheIDPanics(t *testing.T) {
	n := newTestNode(t)
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: "notanumber-1"})
	expectPanic(t, func() { n.processLogEntry(1) })
}

// When a command's reply is already cached, processLogEntry reuses it but then
// tries to cache it again, which CacheClientReply rejects, so it panics. This
// test documents that current behavior.
func TestProcessLogEntry_AlreadyCachedPanics(t *testing.T) {
	n := newTestNode(t)
	if err := n.CacheClientReply("7-1", ClientReply{Status: ClientStatus_OK}); err != nil {
		t.Fatal(err)
	}
	n.StoreLog(&LogEntry{Index: 1, Type: CommandType_STATE_MACHINE_COMMAND, CacheId: "7-1"})
	expectPanic(t, func() { n.processLogEntry(1) })
}

// ---------------------------------------------------------------------------
// doLeader (state loop)
// ---------------------------------------------------------------------------

func newLeader(t *testing.T, term uint64) *Node {
	n := newTestNode(t)
	n.SetCurrentTerm(term)
	n.Config.HeartbeatTimeout = 10 * time.Millisecond
	return n
}

func TestDoLeader_SingleNodeLifecycle(t *testing.T) {
	n := newLeader(t, 3)
	next := runState(n.doLeader)

	// Registration gets committed by the next heartbeat tick.
	r := waitClientReply(t, sendClient(t, n, &ClientRequest{ClientId: 0}))
	if r.Status != ClientStatus_OK || r.ClientId != 2 {
		t.Errorf("registration reply = {status:%v id:%v}, want OK / 2 (index 1 is the NOOP)", r.Status, r.ClientId)
	}

	// State machine command.
	r = waitClientReply(t, sendClient(t, n, &ClientRequest{ClientId: 2, SequenceNum: 1, Data: []byte("a")}))
	if r.Status != ClientStatus_OK || !bytes.Equal(r.Response, []byte("ok:a")) {
		t.Errorf("command reply = {status:%v resp:%q}", r.Status, r.Response)
	}

	// Stale and same-term vote requests are rejected without stepping down.
	for _, term := range []uint64{2, 3} {
		v := waitRVReply(t, sendRV(t, n, &RequestVoteRequest{Term: term, Candidate: &RemoteNode{Id: "c"}}))
		if v.VoteGranted || v.Term != 3 {
			t.Errorf("term %d vote = {granted:%v term:%v}, want rejected at term 3", term, v.VoteGranted, v.Term)
		}
	}

	// Stale AppendEntries is rejected without stepping down.
	a := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 1, Leader: &RemoteNode{Id: "old"}}))
	if a.Success {
		t.Error("stale AppendEntries accepted by leader")
	}

	// Ignored shutdown signal.
	sendExit(t, n, false)
	assertStillRunning(t, next)

	if n.GetState() != LeaderState {
		t.Errorf("state = %v, want LeaderState", n.GetState())
	}
	if l := n.getLeader(); l == nil || l.Id != n.Self.Id {
		t.Errorf("leader = %v, want self", l)
	}

	sendExit(t, n, true)
	if f := waitNext(t, next); f != nil {
		t.Error("doLeader should return nil on graceful exit")
	}

	if e := n.GetLog(1); e == nil || e.Type != CommandType_NOOP || e.TermId != 3 {
		t.Errorf("first leader entry = %v, want NOOP at term 3", e)
	}
	if n.CommitIndex.Load() != 3 {
		t.Errorf("CommitIndex = %v, want 3", n.CommitIndex.Load())
	}
}

func TestDoLeader_InitializesPeerIndices(t *testing.T) {
	n := newLeader(t, 1)
	storeLogs(n, 1, 1)
	dead := deniedPeer(n, "dead")
	n.setPeers([]*RemoteNode{n.Self, dead})
	n.nextIndex[dead.Id], n.matchIndex[dead.Id] = 99, 99

	next := runState(n.doLeader)
	sendExit(t, n, true)
	waitNext(t, next)

	if n.nextIndex[dead.Id] != 3 || n.matchIndex[dead.Id] != 0 {
		t.Errorf("next/match = %v/%v, want 3/0", n.nextIndex[dead.Id], n.matchIndex[dead.Id])
	}
	if n.matchIndex[n.Self.Id] != 3 {
		t.Errorf("matchIndex[self] = %v, want 3", n.matchIndex[n.Self.Id])
	}
}

func TestDoLeader_StepsDownOnHigherTermAppendEntries(t *testing.T) {
	n := newLeader(t, 2)
	n.setPeers([]*RemoteNode{n.Self, deniedPeer(n, "dead")}) // no quorum, nothing commits
	next := runState(n.doLeader)

	pending := sendClient(t, n, &ClientRequest{ClientId: 0})

	usurper := &RemoteNode{Id: "new", Addr: "n"}
	a := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 5, Leader: usurper, PrevLogIndex: 0}))
	if !a.Success {
		t.Error("expected AppendEntries from new leader to succeed")
	}
	if f := waitNext(t, next); !sameState(f, n.doFollower) {
		t.Error("leader should become follower")
	}

	r := waitClientReply(t, pending)
	if r.Status != ClientStatus_NOT_LEADER || r.LeaderHint == nil || r.LeaderHint.Id != "new" {
		t.Errorf("pending reply = {status:%v hint:%v}, want NOT_LEADER / new", r.Status, r.LeaderHint)
	}
	if n.GetCurrentTerm() != 5 {
		t.Errorf("term = %v, want 5", n.GetCurrentTerm())
	}
}

func TestDoLeader_StepsDownOnHigherTermRequestVote(t *testing.T) {
	n := newLeader(t, 2)
	n.setPeers([]*RemoteNode{n.Self, deniedPeer(n, "dead")})
	next := runState(n.doLeader)

	pending := sendClient(t, n, &ClientRequest{ClientId: 0})

	// Candidate's last log term (2) matches ours, and its log is as long, so it gets the vote.
	v := waitRVReply(t, sendRV(t, n, &RequestVoteRequest{
		Term: 4, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 10, LastLogTerm: 2,
	}))
	if !v.VoteGranted || v.Term != 4 {
		t.Errorf("vote = {granted:%v term:%v}, want granted at term 4", v.VoteGranted, v.Term)
	}
	if f := waitNext(t, next); !sameState(f, n.doFollower) {
		t.Error("leader should become follower")
	}

	r := waitClientReply(t, pending)
	if r.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Errorf("pending status = %v, want ELECTION_IN_PROGRESS", r.Status)
	}
	if n.GetVotedFor() != "c" {
		t.Errorf("votedFor = %q, want c", n.GetVotedFor())
	}
}

func TestDoLeader_InitialHeartbeatFallback(t *testing.T) {
	n := newLeader(t, 1)
	p := fakePeer(t, func(req *AppendEntriesRequest) AppendEntriesReply {
		return AppendEntriesReply{Term: 7}
	})
	n.setPeers([]*RemoteNode{n.Self, p.Self})

	ch := make(chan ClientReply, 1)
	n.requestsByCacheID["1-1"] = []chan ClientReply{ch}

	if f := waitNext(t, runState(n.doLeader)); !sameState(f, n.doFollower) {
		t.Fatal("leader should fall back after the first heartbeat sees a higher term")
	}
	if n.GetCurrentTerm() != 7 {
		t.Errorf("term = %v, want 7", n.GetCurrentTerm())
	}
	if r := waitClientReply(t, ch); r.Status != ClientStatus_NOT_LEADER {
		t.Errorf("pending status = %v, want NOT_LEADER", r.Status)
	}
}

func TestDoLeader_TickerHeartbeatFallback(t *testing.T) {
	n := newLeader(t, 1)
	calls := atomic.NewInt32(0)
	p := fakePeer(t, func(req *AppendEntriesRequest) AppendEntriesReply {
		if calls.Inc() < 3 {
			return AppendEntriesReply{Term: 1, Success: true}
		}
		return AppendEntriesReply{Term: 8}
	})
	n.setPeers([]*RemoteNode{n.Self, p.Self})

	if f := waitNext(t, runState(n.doLeader)); !sameState(f, n.doFollower) {
		t.Fatal("leader should fall back once a periodic heartbeat sees a higher term")
	}
	if calls.Load() < 3 {
		t.Errorf("peer saw %d heartbeats, want at least 3", calls.Load())
	}
	if n.GetCurrentTerm() != 8 {
		t.Errorf("term = %v, want 8", n.GetCurrentTerm())
	}
	// The NOOP was replicated to the peer before the fallback, so it committed.
	if n.CommitIndex.Load() != 1 {
		t.Errorf("CommitIndex = %v, want 1", n.CommitIndex.Load())
	}
}
