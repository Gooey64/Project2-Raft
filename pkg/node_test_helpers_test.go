package pkg

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// waitTimeout bounds every blocking receive in the tests so a bug shows up as a
// failure instead of a hung test binary.
const waitTimeout = 3 * time.Second

// fakeStateMachine is a minimal StateMachine used to observe applied commands.
type fakeStateMachine struct {
	mu      sync.Mutex
	fail    bool
	applied [][]byte
}

func (f *fakeStateMachine) GetState() interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

func (f *fakeStateMachine) ApplyCommand(command uint64, data []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("apply failed")
	}
	f.applied = append(f.applied, data)
	return append([]byte("ok:"), data...), nil
}

// newTestNode creates a node backed by an in-memory store and a fake state
// machine. It deliberately does NOT call n.Exit() on cleanup: Exit calls
// os.Exit(0), which would silently end the whole test binary.
func newTestNode(t *testing.T) *Node {
	t.Helper()
	n, err := CreateNode(OpenPort(0), DefaultConfig(), &fakeStateMachine{}, NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		n.cancel()
		n.Server.Stop()
	})
	return n
}

// fakePeer starts a real gRPC-backed node whose appendEntries channel is
// drained by handler instead of a state function, so a leader under test can
// talk to it over the network and receive scripted replies.
func fakePeer(t *testing.T, handler func(req *AppendEntriesRequest) AppendEntriesReply) *Node {
	t.Helper()
	return fakeVoter(t, handler, nil)
}

// fakeVoter is fakePeer that also answers RequestVote RPCs with rvHandler.
// A nil handler replies with an unsuccessful response at term 0. Handlers may
// be called from several goroutines at once (a candidate may request votes in
// parallel), so they must be safe for concurrent use.
func fakeVoter(t *testing.T,
	aeHandler func(req *AppendEntriesRequest) AppendEntriesReply,
	rvHandler func(req *RequestVoteRequest) RequestVoteReply,
) *Node {
	t.Helper()
	p, err := CreateNode(OpenPort(0), DefaultConfig(), &fakeStateMachine{}, NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-p.appendEntries:
				if aeHandler == nil {
					msg.reply <- AppendEntriesReply{}
				} else {
					msg.reply <- aeHandler(msg.request)
				}
			case msg := <-p.requestVote:
				if rvHandler == nil {
					msg.reply <- RequestVoteReply{}
				} else {
					msg.reply <- rvHandler(msg.request)
				}
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		// Stop the server while the drain goroutine is still running so no
		// in-flight handler is left blocked on the unbuffered channel.
		p.cancel()
		p.Server.Stop()
		close(done)
	})
	return p
}

// deniedPeer returns a peer that n can never reach: the network policy rejects
// every RPC from n to it immediately, without dialing.
func deniedPeer(n *Node, id string) *RemoteNode {
	peer := &RemoteNode{Id: id, Addr: "127.0.0.1:1"}
	n.NetworkPolicy.RegisterPolicy(n.Self, peer, false)
	return peer
}

// sameState reports whether two state functions are the same method.
func sameState(a, b stateFunction) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// runState runs a state function in the background and returns a channel that
// receives the next state function once it returns.
func runState(f stateFunction) <-chan stateFunction {
	next := make(chan stateFunction, 1)
	go func() { next <- f() }()
	return next
}

func waitNext(t *testing.T, next <-chan stateFunction) stateFunction {
	t.Helper()
	select {
	case f := <-next:
		return f
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for state function to return")
		return nil
	}
}

func assertStillRunning(t *testing.T, next <-chan stateFunction) {
	t.Helper()
	select {
	case <-next:
		t.Fatal("state function returned unexpectedly")
	case <-time.After(20 * time.Millisecond):
	}
}

func waitClientReply(t *testing.T, ch <-chan ClientReply) ClientReply {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for client reply")
		return ClientReply{}
	}
}

func waitAEReply(t *testing.T, ch <-chan AppendEntriesReply) AppendEntriesReply {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for AppendEntries reply")
		return AppendEntriesReply{}
	}
}

func waitRVReply(t *testing.T, ch <-chan RequestVoteReply) RequestVoteReply {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for RequestVote reply")
		return RequestVoteReply{}
	}
}

// The send* helpers deliver a message to a running state function, failing if
// nothing reads it, and return the reply channel.
func sendAE(t *testing.T, n *Node, req *AppendEntriesRequest) <-chan AppendEntriesReply {
	t.Helper()
	reply := make(chan AppendEntriesReply, 1)
	select {
	case n.appendEntries <- AppendEntriesMsg{req, reply}:
	case <-time.After(waitTimeout):
		t.Fatal("timed out sending AppendEntries")
	}
	return reply
}

func sendRV(t *testing.T, n *Node, req *RequestVoteRequest) <-chan RequestVoteReply {
	t.Helper()
	reply := make(chan RequestVoteReply, 1)
	select {
	case n.requestVote <- RequestVoteMsg{req, reply}:
	case <-time.After(waitTimeout):
		t.Fatal("timed out sending RequestVote")
	}
	return reply
}

func sendClient(t *testing.T, n *Node, req *ClientRequest) <-chan ClientReply {
	t.Helper()
	reply := make(chan ClientReply, 1)
	select {
	case n.clientRequest <- ClientRequestMsg{req, reply}:
	case <-time.After(waitTimeout):
		t.Fatal("timed out sending client request")
	}
	return reply
}

func sendExit(t *testing.T, n *Node, shutdown bool) {
	t.Helper()
	select {
	case n.gracefulExit <- shutdown:
	case <-time.After(waitTimeout):
		t.Fatal("timed out sending graceful exit")
	}
}

// voteRecorder is a concurrency-safe RequestVote handler that records every
// request and answers with the given grant decision at the request's term
// (or at replyTerm if it is non-zero).
type voteRecorder struct {
	mu        sync.Mutex
	grant     bool
	replyTerm uint64
	requests  []*RequestVoteRequest
}

func (v *voteRecorder) handle(req *RequestVoteRequest) RequestVoteReply {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.requests = append(v.requests, req)
	term := req.Term
	if v.replyTerm != 0 {
		term = v.replyTerm
	}
	return RequestVoteReply{Term: term, VoteGranted: v.grant}
}

func (v *voteRecorder) seen() []*RequestVoteRequest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]*RequestVoteRequest(nil), v.requests...)
}

// eventually polls cond until it is true or waitTimeout elapses.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func expectPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic, got none")
		}
	}()
	f()
}

// storeLogs appends entries with the given terms at indices 1..len(terms).
func storeLogs(n *Node, terms ...uint64) {
	for i, term := range terms {
		n.StoreLog(&LogEntry{Index: uint64(i + 1), TermId: term, Type: CommandType_NOOP})
	}
}
