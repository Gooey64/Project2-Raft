package pkg

import (
	"testing"
	"time"
)

// These tests specify the expected behavior of a correct candidate
// implementation (Raft paper, figure 2 and section 5.2).
//
// Where the starter code leaves room for more than one valid design, the tests
// accept any of them:
//   - On a lost or split election, doCandidate may either loop internally and
//     start a new election, or return n.doCandidate. Both are accepted.
//   - A candidate may answer client requests with NOT_LEADER or
//     ELECTION_IN_PROGRESS (the same pair test/example_client_test.go allows).

// newCandidate returns a node at term 1 whose election timeout is long enough
// that it never starts a second election during the test, unless a test
// shortens it.
func newCandidate(t *testing.T) *Node {
	n := newTestNode(t)
	n.SetCurrentTerm(1)
	n.Config.ElectionTimeout = time.Hour
	return n
}

// stuckCandidate returns a candidate in a 3-node cluster whose two peers are
// unreachable, so it can never win and stays in its candidate loop.
func stuckCandidate(t *testing.T) (*Node, <-chan stateFunction) {
	n := newCandidate(t)
	n.setPeers([]*RemoteNode{n.Self, deniedPeer(n, "p1"), deniedPeer(n, "p2")})
	next := runState(n.doCandidate)
	eventually(t, "candidate to start its election", func() bool {
		return n.GetState() == CandidateState && n.GetCurrentTerm() == 2
	})
	return n, next
}

// stopCandidate shuts down a running candidate. It tolerates an implementation
// that returns n.doCandidate between elections by restarting it.
func stopCandidate(t *testing.T, n *Node, next <-chan stateFunction) {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		select {
		case n.gracefulExit <- true:
			if f := waitNext(t, next); f != nil && !sameState(f, n.doCandidate) {
				t.Errorf("candidate should return nil on graceful exit")
			}
			return
		case f := <-next:
			if !sameState(f, n.doCandidate) {
				return // already left the candidate state
			}
			next = runState(n.doCandidate)
		case <-deadline:
			t.Fatal("timed out stopping candidate")
		}
	}
}

// ---------------------------------------------------------------------------
// doCandidate: starting an election
// ---------------------------------------------------------------------------

func TestDoCandidate_StartsElection(t *testing.T) {
	n, next := stuckCandidate(t)
	defer stopCandidate(t, n, next)

	if n.GetState() != CandidateState {
		t.Errorf("state = %v, want CandidateState", n.GetState())
	}
	if n.GetCurrentTerm() != 2 {
		t.Errorf("term = %v, want 2 (incremented from 1)", n.GetCurrentTerm())
	}
	if n.GetVotedFor() != n.Self.Id {
		t.Errorf("votedFor = %q, candidate must vote for itself", n.GetVotedFor())
	}
}

func TestDoCandidate_SingleNodeWins(t *testing.T) {
	n := newCandidate(t)

	f := waitNext(t, runState(n.doCandidate))
	if !sameState(f, n.doLeader) {
		t.Fatal("a single-node cluster should elect itself leader")
	}
	if n.GetCurrentTerm() != 2 || n.GetVotedFor() != n.Self.Id {
		t.Errorf("term/votedFor = %v/%q, want 2/self", n.GetCurrentTerm(), n.GetVotedFor())
	}
}

func TestDoCandidate_WinsWithMajority(t *testing.T) {
	n := newCandidate(t)
	storeLogs(n, 1, 1)

	v1, v2 := &voteRecorder{grant: true}, &voteRecorder{grant: true}
	p1, p2 := fakeVoter(t, nil, v1.handle), fakeVoter(t, nil, v2.handle)
	n.setPeers([]*RemoteNode{n.Self, p1.Self, p2.Self})

	if f := waitNext(t, runState(n.doCandidate)); !sameState(f, n.doLeader) {
		t.Fatal("candidate with all votes should become leader")
	}

	for i, v := range []*voteRecorder{v1, v2} {
		reqs := v.seen()
		if len(reqs) == 0 {
			t.Fatalf("peer %d received no RequestVote", i+1)
		}
		r := reqs[0]
		if r.Term != 2 || r.Candidate.Id != n.Self.Id || r.LastLogIndex != 2 || r.LastLogTerm != 1 {
			t.Errorf("peer %d request = {term:%v cand:%v lastIdx:%v lastTerm:%v}, want {2 self 2 1}",
				i+1, r.Term, r.Candidate.Id, r.LastLogIndex, r.LastLogTerm)
		}
	}
}

func TestDoCandidate_WinsDespiteUnreachablePeer(t *testing.T) {
	n := newCandidate(t)
	v := &voteRecorder{grant: true}
	p := fakeVoter(t, nil, v.handle)
	n.setPeers([]*RemoteNode{n.Self, p.Self, deniedPeer(n, "dead")})

	if f := waitNext(t, runState(n.doCandidate)); !sameState(f, n.doLeader) {
		t.Fatal("2 of 3 votes is a majority; candidate should become leader")
	}
}

func TestDoCandidate_HigherTermVoteReplyFallsBack(t *testing.T) {
	n := newCandidate(t)
	v := &voteRecorder{grant: false, replyTerm: 9}
	p := fakeVoter(t, nil, v.handle)
	n.setPeers([]*RemoteNode{n.Self, p.Self, deniedPeer(n, "dead")})

	if f := waitNext(t, runState(n.doCandidate)); !sameState(f, n.doFollower) {
		t.Fatal("candidate should become follower after seeing a higher term")
	}
	if n.GetCurrentTerm() != 9 {
		t.Errorf("term = %v, want 9", n.GetCurrentTerm())
	}
}

func TestDoCandidate_LostElectionRetriesWithHigherTerm(t *testing.T) {
	n := newCandidate(t)
	n.Config.ElectionTimeout = 20 * time.Millisecond
	v1, v2 := &voteRecorder{grant: false}, &voteRecorder{grant: false}
	p1, p2 := fakeVoter(t, nil, v1.handle), fakeVoter(t, nil, v2.handle)
	n.setPeers([]*RemoteNode{n.Self, p1.Self, p2.Self})

	next := runState(n.doCandidate)
	deadline := time.After(waitTimeout)
	for n.GetCurrentTerm() < 3 {
		select {
		case f := <-next:
			if !sameState(f, n.doCandidate) {
				t.Fatal("a lost election must not leave the candidate state")
			}
			next = runState(n.doCandidate)
		case <-deadline:
			t.Fatalf("no new election after timeout; term stuck at %v", n.GetCurrentTerm())
		case <-time.After(2 * time.Millisecond):
		}
	}
	defer stopCandidate(t, n, next)

	eventually(t, "peers to see a RequestVote for term 3", func() bool {
		for _, r := range v1.seen() {
			if r.Term >= 3 {
				return true
			}
		}
		return false
	})
	if n.GetState() != CandidateState {
		t.Errorf("state = %v, want CandidateState", n.GetState())
	}
}

// ---------------------------------------------------------------------------
// doCandidate: messages received while waiting for votes
// ---------------------------------------------------------------------------

func TestDoCandidate_AppendEntriesFromNewLeaderFallsBack(t *testing.T) {
	n, next := stuckCandidate(t)

	leader := &RemoteNode{Id: "L", Addr: "l"}
	r := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 2, Leader: leader}))
	if !r.Success {
		t.Error("candidate should accept AppendEntries from a leader of its own term")
	}
	if f := waitNext(t, next); !sameState(f, n.doFollower) {
		t.Fatal("candidate should become follower when a leader of its term appears")
	}
	if l := n.getLeader(); l == nil || l.Id != "L" {
		t.Errorf("leader = %v, want L", l)
	}
}

func TestDoCandidate_HigherTermAppendEntriesFallsBack(t *testing.T) {
	n, next := stuckCandidate(t)

	r := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 5, Leader: &RemoteNode{Id: "L"}}))
	if !r.Success || r.Term != 5 {
		t.Errorf("reply = {term:%v success:%v}, want {5 true}", r.Term, r.Success)
	}
	if f := waitNext(t, next); !sameState(f, n.doFollower) {
		t.Fatal("candidate should become follower on higher-term AppendEntries")
	}
}

func TestDoCandidate_StaleAppendEntriesRejected(t *testing.T) {
	n, next := stuckCandidate(t)
	defer stopCandidate(t, n, next)

	r := waitAEReply(t, sendAE(t, n, &AppendEntriesRequest{Term: 1, Leader: &RemoteNode{Id: "old"}}))
	if r.Success || r.Term != 2 {
		t.Errorf("reply = {term:%v success:%v}, want {2 false}", r.Term, r.Success)
	}
	assertStillRunning(t, next)
	if n.GetState() != CandidateState {
		t.Errorf("state = %v, want CandidateState", n.GetState())
	}
}

func TestDoCandidate_HigherTermRequestVoteFallsBack(t *testing.T) {
	n, next := stuckCandidate(t)

	r := waitRVReply(t, sendRV(t, n, &RequestVoteRequest{Term: 4, Candidate: &RemoteNode{Id: "c"}}))
	if !r.VoteGranted || r.Term != 4 {
		t.Errorf("reply = {term:%v granted:%v}, want {4 true}", r.Term, r.VoteGranted)
	}
	if f := waitNext(t, next); !sameState(f, n.doFollower) {
		t.Fatal("candidate should become follower on higher-term RequestVote")
	}
	if n.GetCurrentTerm() != 4 || n.GetVotedFor() != "c" {
		t.Errorf("term/votedFor = %v/%q, want 4/c", n.GetCurrentTerm(), n.GetVotedFor())
	}
}

func TestDoCandidate_RivalSameTermRequestVoteRejected(t *testing.T) {
	n, next := stuckCandidate(t)
	defer stopCandidate(t, n, next)

	r := waitRVReply(t, sendRV(t, n, &RequestVoteRequest{Term: 2, Candidate: &RemoteNode{Id: "rival"}}))
	if r.VoteGranted {
		t.Error("candidate already voted for itself this term")
	}
	assertStillRunning(t, next)
	if n.GetVotedFor() != n.Self.Id {
		t.Errorf("votedFor = %q, want self", n.GetVotedFor())
	}
}

func TestDoCandidate_ClientRequestRejected(t *testing.T) {
	n, next := stuckCandidate(t)
	defer stopCandidate(t, n, next)

	r := waitClientReply(t, sendClient(t, n, &ClientRequest{ClientId: 1, SequenceNum: 1}))
	if r.Status != ClientStatus_NOT_LEADER && r.Status != ClientStatus_ELECTION_IN_PROGRESS {
		t.Errorf("status = %v, want NOT_LEADER or ELECTION_IN_PROGRESS", r.Status)
	}
	assertStillRunning(t, next)
}

func TestDoCandidate_GracefulExit(t *testing.T) {
	n, next := stuckCandidate(t)

	sendExit(t, n, false)
	assertStillRunning(t, next)

	sendExit(t, n, true)
	if f := waitNext(t, next); f != nil {
		t.Error("doCandidate should return nil on graceful exit")
	}
}

// ---------------------------------------------------------------------------
// requestVotes
// ---------------------------------------------------------------------------

// electing prepares n as a candidate that has already started an election for
// term 2 (incremented its term and voted for itself).
func electing(n *Node) {
	n.SetCurrentTerm(2)
	n.setVotedFor(n.Self.Id)
}

func TestRequestVotes(t *testing.T) {
	tests := []struct {
		name         string
		grants       []bool // one fake peer per entry
		unreachable  int    // extra peers that cannot be contacted
		wantFallback bool
		wantWon      bool
	}{
		{"single node wins", nil, 0, false, true},
		{"all peers grant", []bool{true, true}, 0, false, true},
		{"all peers reject", []bool{false, false}, 0, false, false},
		{"one of two peers grants (2/3)", []bool{true, false}, 0, false, true},
		{"all peers unreachable", nil, 2, false, false},
		{"exactly half is not a majority (2/4)", []bool{true, false, false}, 0, false, false},
		{"bare majority of five (3/5)", []bool{true, true, false, false}, 0, false, true},
		{"one grant plus unreachable (2/3)", []bool{true}, 1, false, true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(t)
			electing(n)
			peers := []*RemoteNode{n.Self}
			for _, g := range tc.grants {
				v := &voteRecorder{grant: g}
				peers = append(peers, fakeVoter(t, nil, v.handle).Self)
			}
			for i := 0; i < tc.unreachable; i++ {
				peers = append(peers, deniedPeer(n, "dead"+string(rune('a'+i))))
			}
			n.setPeers(peers)

			fallback, won := n.requestVotes(2)
			if fallback != tc.wantFallback || won != tc.wantWon {
				t.Errorf("requestVotes = (fallback:%v, won:%v), want (%v, %v)",
					fallback, won, tc.wantFallback, tc.wantWon)
			}
			if n.GetCurrentTerm() != 2 {
				t.Errorf("term changed to %v without seeing a higher term", n.GetCurrentTerm())
			}
		})
	}
}

func TestRequestVotes_SendsCorrectRequest(t *testing.T) {
	n := newTestNode(t)
	storeLogs(n, 1, 1, 2)
	electing(n)
	v := &voteRecorder{grant: true}
	n.setPeers([]*RemoteNode{n.Self, fakeVoter(t, nil, v.handle).Self})

	n.requestVotes(2)

	reqs := v.seen()
	if len(reqs) != 1 {
		t.Fatalf("peer received %d RequestVotes, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Term != 2 || r.Candidate.Id != n.Self.Id || r.LastLogIndex != 3 || r.LastLogTerm != 2 {
		t.Errorf("request = {term:%v cand:%v lastIdx:%v lastTerm:%v}, want {2 self 3 2}",
			r.Term, r.Candidate.Id, r.LastLogIndex, r.LastLogTerm)
	}
}

func TestRequestVotes_DoesNotContactSelf(t *testing.T) {
	n := newTestNode(t)
	electing(n)
	// If the node sent itself a RequestVote, nothing would read n.requestVote
	// and the call would block until the RPC timeout.
	start := time.Now()
	n.requestVotes(2)
	if d := time.Since(start); d > time.Second {
		t.Errorf("requestVotes took %v on a single node; it may be contacting itself", d)
	}
}

func TestRequestVotes_HigherTermFallsBack(t *testing.T) {
	n := newTestNode(t)
	electing(n)
	v := &voteRecorder{grant: false, replyTerm: 9}
	n.setPeers([]*RemoteNode{n.Self, fakeVoter(t, nil, v.handle).Self, deniedPeer(n, "dead")})

	fallback, won := n.requestVotes(2)
	if !fallback || won {
		t.Errorf("requestVotes = (fallback:%v, won:%v), want (true, false)", fallback, won)
	}
	if n.GetCurrentTerm() != 9 {
		t.Errorf("term = %v, want 9", n.GetCurrentTerm())
	}
	if n.GetVotedFor() != "" {
		t.Errorf("votedFor = %q, want cleared for the new term", n.GetVotedFor())
	}
}

// ---------------------------------------------------------------------------
// handleRequestVote (candidate)
// ---------------------------------------------------------------------------

func TestCandidateHandleRequestVote(t *testing.T) {
	tests := []struct {
		name         string
		req          *RequestVoteRequest
		wantGrant    bool
		wantFallback bool
		wantTerm     uint64
		wantVotedFor string // "self" means the candidate's own id
	}{
		{
			name:         "lower term rejected, stay candidate",
			req:          &RequestVoteRequest{Term: 2, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 9, LastLogTerm: 9},
			wantGrant:    false,
			wantFallback: false,
			wantTerm:     3,
			wantVotedFor: "self",
		},
		{
			name:         "same term rival rejected, stay candidate",
			req:          &RequestVoteRequest{Term: 3, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 9, LastLogTerm: 9},
			wantGrant:    false,
			wantFallback: false,
			wantTerm:     3,
			wantVotedFor: "self",
		},
		{
			name:         "higher term with up-to-date log: grant and fall back",
			req:          &RequestVoteRequest{Term: 4, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 1, LastLogTerm: 3},
			wantGrant:    true,
			wantFallback: true,
			wantTerm:     4,
			wantVotedFor: "c",
		},
		{
			name:         "higher term with stale log: reject but still fall back",
			req:          &RequestVoteRequest{Term: 4, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 5, LastLogTerm: 1},
			wantGrant:    false,
			wantFallback: true,
			wantTerm:     4,
			wantVotedFor: "",
		},
		{
			name:         "higher term, same last term but shorter log: reject but fall back",
			req:          &RequestVoteRequest{Term: 4, Candidate: &RemoteNode{Id: "c"}, LastLogIndex: 0, LastLogTerm: 3},
			wantGrant:    false,
			wantFallback: true,
			wantTerm:     4,
			wantVotedFor: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(t)
			n.StoreLog(&LogEntry{Index: 1, TermId: 3})
			n.SetCurrentTerm(3)
			n.setVotedFor(n.Self.Id)

			reply := make(chan RequestVoteReply, 1)
			fallback := n.handleRequestVote(RequestVoteMsg{tc.req, reply})
			r := waitRVReply(t, reply)

			if r.VoteGranted != tc.wantGrant {
				t.Errorf("VoteGranted = %v, want %v", r.VoteGranted, tc.wantGrant)
			}
			if fallback != tc.wantFallback {
				t.Errorf("fallback = %v, want %v", fallback, tc.wantFallback)
			}
			if r.Term != tc.wantTerm || n.GetCurrentTerm() != tc.wantTerm {
				t.Errorf("reply.Term/currentTerm = %v/%v, want %v", r.Term, n.GetCurrentTerm(), tc.wantTerm)
			}
			want := tc.wantVotedFor
			if want == "self" {
				want = n.Self.Id
			}
			if got := n.GetVotedFor(); got != want {
				t.Errorf("votedFor = %q, want %q", got, want)
			}
		})
	}
}
