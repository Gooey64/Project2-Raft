package pkg

import (
	"strconv"
	"strings"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	n.setLeader(n.Self)

	n.LeaderMutex.Lock()
	lastIndex := n.LastLogIndex()
	for _, peer := range n.getPeers() {
		n.nextIndex[peer.Id] = lastIndex + 1
		n.matchIndex[peer.Id] = 0
	}
	n.LeaderMutex.Unlock()

	n.StoreLog(&LogEntry{
		Index:  n.LastLogIndex() + 1,
		TermId: n.GetCurrentTerm(),
		Type:   CommandType_NOOP,
	})
	n.matchIndex[n.Self.Id] = n.LastLogIndex()

	if fallback := n.sendHeartbeats(); fallback {
		n.stepDownFromLeader(ClientStatus_NOT_LEADER)
		return n.doFollower
	}

	ticker := time.NewTicker(n.Config.HeartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if fallback := n.sendHeartbeats(); fallback {
				n.stepDownFromLeader(ClientStatus_NOT_LEADER)
				return n.doFollower
			}

		case msg := <-n.appendEntries:
			if _, fallback := n.handleAppendEntries(msg); fallback {
				n.stepDownFromLeader(ClientStatus_NOT_LEADER)
				return n.doFollower
			}

		case msg := <-n.requestVote:
			if msg.request.Term > n.GetCurrentTerm() {
				n.followerHandleRequestVote(msg)
				n.stepDownFromLeader(ClientStatus_ELECTION_IN_PROGRESS)
				return n.doFollower
			}
			msg.reply <- RequestVoteReply{
				Term:        n.GetCurrentTerm(),
				VoteGranted: false,
			}

		case msg := <-n.clientRequest:
			n.leaderHandleClientRequest(msg)

		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// leaderHandleClientRequest appends a client's request to the leader's log and
// registers the client's reply channel so processLogEntry can answer it once
// the entry has been committed and applied.
func (n *Node) leaderHandleClientRequest(msg ClientRequestMsg) {
	req := msg.request
	nextIndex := n.LastLogIndex() + 1

	var entry *LogEntry
	if req.ClientId == 0 {
		entry = &LogEntry{
			Index:   nextIndex,
			TermId:  n.GetCurrentTerm(),
			Type:    CommandType_CLIENT_REGISTRATION,
			CacheId: CreateCacheID(0, nextIndex),
		}
	} else {
		cacheID := CreateCacheID(req.ClientId, req.SequenceNum)

		if cached, ok := n.GetCachedReply(cacheID); ok {
			msg.reply <- *cached
			return
		}

		n.requestsMutex.Lock()
		if _, pending := n.requestsByCacheID[cacheID]; pending {
			n.requestsByCacheID[cacheID] = append(n.requestsByCacheID[cacheID], msg.reply)
			n.requestsMutex.Unlock()
			return
		}
		n.requestsMutex.Unlock()

		entry = &LogEntry{
			Index:   nextIndex,
			TermId:  n.GetCurrentTerm(),
			Type:    CommandType_STATE_MACHINE_COMMAND,
			Command: req.StateMachineCmd,
			Data:    req.Data,
			CacheId: cacheID,
		}
	}

	n.requestsMutex.Lock()
	n.requestsByCacheID[entry.CacheId] = append(n.requestsByCacheID[entry.CacheId], msg.reply)
	n.requestsMutex.Unlock()

	n.StoreLog(entry)
	n.matchIndex[n.Self.Id] = n.LastLogIndex()
}

// stepDownFromLeader tells every client still waiting on this leader that it
// is no longer the leader, so they retry elsewhere instead of hanging.
func (n *Node) stepDownFromLeader(status ClientStatus) {
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	for cacheID, replies := range n.requestsByCacheID {
		for _, ch := range replies {
			ch <- ClientReply{
				Status:     status,
				LeaderHint: n.getLeader(),
			}
		}
		delete(n.requestsByCacheID, cacheID)
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
//
// If another node isn't up-to-date, then the leader should attempt to
// update them, and, if an index has made it to a quorum of nodes, commit
// up to that index. Once committed to that index, the replicated state
// machine should be given the new log entries via processLogEntry.
func (n *Node) sendHeartbeats() (fallback bool) {
	currentTerm := n.GetCurrentTerm()
	peers := n.getPeers()

	type result struct {
		peerID    string
		prevIndex uint64
		sent      int
		reply     *AppendEntriesReply
	}

	var others []*RemoteNode
	for _, peer := range peers {
		if peer.Id != n.Self.Id {
			others = append(others, peer)
		}
	}

	// Send to every peer in parallel. An RPC to a partitioned peer can take the
	// full RPC timeout (2s), so sending one at a time would starve the reachable
	// followers of heartbeats and make them start elections.
	// Buffered so late senders never block after we stop listening.
	results := make(chan result, len(others))
	for _, peer := range others {
		// for keeping track of entries a follower is missing
		var entries []*LogEntry
		for i := n.nextIndex[peer.Id]; i <= n.LastLogIndex(); i++ {
			entries = append(entries, n.GetLog(i))
		}

		prevIndex := n.nextIndex[peer.Id] - 1

		request := &AppendEntriesRequest{
			Term:         currentTerm,
			Leader:       n.Self,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  n.GetLog(prevIndex).TermId,
			Entries:      entries,
			LeaderCommit: n.CommitIndex.Load(),
		}

		go func(peer *RemoteNode, request *AppendEntriesRequest, prevIndex uint64, sent int) {
			reply, err := peer.AppendEntriesRPC(n, request)
			if err != nil {
				reply = nil
			}
			results <- result{peer.Id, prevIndex, sent, reply}
		}(peer, request, prevIndex, len(entries))
	}

	// Wait for replies, but never longer than one heartbeat interval. Slow
	// (partitioned) peers are simply retried on the next heartbeat.
	// nextIndex/matchIndex are only touched here, on the leader's goroutine.
	deadline := time.After(n.Config.HeartbeatTimeout)
collect:
	for range others {
		select {
		case r := <-results:
			if r.reply == nil {
				continue
			}
			if r.reply.Term > currentTerm {
				if r.reply.Term > n.GetCurrentTerm() {
					n.SetCurrentTerm(r.reply.Term)
					n.setVotedFor("")
				}
				return true
			}
			if r.reply.Success {
				n.matchIndex[r.peerID] = r.prevIndex + uint64(r.sent)
				n.nextIndex[r.peerID] = n.matchIndex[r.peerID] + 1
			} else if n.nextIndex[r.peerID] > 1 {
				n.nextIndex[r.peerID]--
			}
		case <-deadline:
			break collect
		}
	}

	// commits the highest index a majority has replicated in the current term

	for N := n.LastLogIndex(); N > n.CommitIndex.Load(); N-- {
		if n.GetLog(N).TermId != currentTerm {
			break
		}

		count := 1
		for _, peer := range peers {
			if peer.Id != n.Self.Id && n.matchIndex[peer.Id] >= N {
				count++
			}
		}

		if count > len(peers)/2 {
			for n.LastApplied.Load() < N {
				n.processLogEntry(n.LastApplied.Inc())
			}
			n.CommitIndex.Store(N)
			break
		}
	}
	return false
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}
