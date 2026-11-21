package pkg

import "testing"

func newTestNode(t *testing.T) *Node {
	n, err := CreateNode(OpenPort(0), DefaultConfig(), nil, NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func vote(n *Node, req *RequestVoteRequest) (RequestVoteReply, bool) {
	reply := make(chan RequestVoteReply, 1)
	reset, _ := n.followerHandleRequestVote(RequestVoteMsg{req, reply})
	return <-reply, reset
}

func testFollowerRequestVote(t *testing.T) {
	cand := &RemoteNode{Id: "cand", Addr: "x"}

	tests := []struct {
		name      string
		setup     func(n *Node)
		req       *RequestVoteRequest
		wantGrant bool
	}{
		{"lower term rejected",
			func(n *Node) { n.SetCurrentTerm(5) },
			&RequestVoteRequest{Term: 4, Candidate: cand},
			false},
		{"already voted for someone else this term",
			func(n *Node) { n.SetCurrentTerm(2); n.setVotedFor("other") },
			&RequestVoteRequest{Term: 2, Candidate: cand},
			false},
		{"higher last-log term wins despite shorter log",
			func(n *Node) {
				n.SetCurrentTerm(1)
				n.StoreLog(&LogEntry{Index: 1, TermId: 1})
				n.StoreLog(&LogEntry{Index: 2, TermId: 1})
			},
			&RequestVoteRequest{Term: 3, Candidate: cand, LastLogIndex: 1, LastLogTerm: 3},
			true},
		{"same last term, shorter log rejected",
			func(n *Node) {
				n.SetCurrentTerm(1)
				n.StoreLog(&LogEntry{Index: 1, TermId: 1})
				n.StoreLog(&LogEntry{Index: 2, TermId: 1})
			},
			&RequestVoteRequest{Term: 2, Candidate: cand, LastLogIndex: 1, LastLogTerm: 1},
			false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(t)
			defer n.Exit()
			tc.setup(n)

			reply, reset := vote(n, tc.req)
			if reply.VoteGranted != tc.wantGrant {
				t.Fatalf("VoteGranted = %v, want %v", reply.VoteGranted, tc.wantGrant)
			}
			if reset != tc.wantGrant {
				t.Errorf("resetTimeout = %v, want %v", reset, tc.wantGrant)
			}
			if tc.wantGrant && n.GetVotedFor() != cand.Id {
				t.Errorf("votedFor = %q, want %q", n.GetVotedFor(), cand.Id)
			}
		})
	}
}

// func appendEntries(n *Node, req *AppendEntriesRequest) (AppendEntriesReply, bool) {
// 	reply := make(chan AppendEntriesReply, 1)
// 	reset, _ := n.handleAppendEntries(RequestVoteMsg{req, reply})
// 	return <-reply, reset
// }

// func testFollowerRequestVote(t *testing.T) {
// 	cand := &RemoteNode{Id: "cand", Addr: "x"}

// 	tests := []struct {
// 		name      string
// 		setup     func(n *Node)
// 		req       *RequestVoteRequest
// 		wantGrant bool
// 	}{
// 		{"lower term rejected",
// 			func(n *Node) { n.SetCurrentTerm(5) },
// 			&RequestVoteRequest{Term: 4, Candidate: cand},
// 			false},
// 		{"already voted for someone else this term",
// 			func(n *Node) { n.SetCurrentTerm(2); n.setVotedFor("other") },
// 			&RequestVoteRequest{Term: 2, Candidate: cand},
// 			false},
// 		{"higher last-log term wins despite shorter log",
// 			func(n *Node) {
// 				n.SetCurrentTerm(1)
// 				n.StoreLog(&LogEntry{Index: 1, TermId: 1})
// 				n.StoreLog(&LogEntry{Index: 2, TermId: 1})
// 			},
// 			&RequestVoteRequest{Term: 3, Candidate: cand, LastLogIndex: 1, LastLogTerm: 3},
// 			true},
// 		{"same last term, shorter log rejected",
// 			func(n *Node) {
// 				n.SetCurrentTerm(1)
// 				n.StoreLog(&LogEntry{Index: 1, TermId: 1})
// 				n.StoreLog(&LogEntry{Index: 2, TermId: 1})
// 			},
// 			&RequestVoteRequest{Term: 2, Candidate: cand, LastLogIndex: 1, LastLogTerm: 1},
// 			false},
// 	}

// 	for _, tc := range tests {
// 		t.Run(tc.name, func(t *testing.T) {
// 			n := newTestNode(t)
// 			defer n.Exit()
// 			tc.setup(n)

// 			reply, reset := vote(n, tc.req)
// 			if reply.VoteGranted != tc.wantGrant {
// 				t.Fatalf("VoteGranted = %v, want %v", reply.VoteGranted, tc.wantGrant)
// 			}
// 			if reset != tc.wantGrant {
// 				t.Errorf("resetTimeout = %v, want %v", reset, tc.wantGrant)
// 			}
// 			if tc.wantGrant && n.GetVotedFor() != cand.Id {
// 				t.Errorf("votedFor = %q, want %q", n.GetVotedFor(), cand.Id)
// 			}
// 		})
// 	}
// }