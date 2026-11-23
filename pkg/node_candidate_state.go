package pkg

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {

	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// Want a new election, so increment and immediately vote for self
	n.SetCurrentTerm(n.GetCurrentTerm() + 1)
	n.setVotedFor(n.Self.Id)
	currTerm := n.GetCurrentTerm()

	timeout := randomTimeout(n.Config.ElectionTimeout)

	type voteResult struct {
		fallback, won bool
	}

	results := make(chan voteResult, 1)

	// Should collect votes so we can keep collecting messages
	go func() {
		fb, won := n.requestVotes(currTerm)
		results <- voteResult{fallback: fb, won: won}
	}()

	for {
		select {
		case res :=<-results:
			if res.fallback {
				return n.doFollower
			}
			if res.won {
				return n.doLeader
			}

			// Fine if election lost or timeout, will start another election

		case <-timeout:
			return n.doCandidate

		case msg := <-n.appendEntries:
			if  _, fallback:= n.handleAppendEntries(msg); fallback {
				return n.doFollower
			}

		case msg := <-n.requestVote:
			if n.handleRequestVote(msg){
				return n.doFollower
			}

		case msg := <-n.clientRequest:
			msg.reply <- ClientReply{Status: ClientStatus_ELECTION_IN_PROGRESS, LeaderHint: n.getLeader()}

		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {

	// Checks the real field name
	lastIndex := n.LastLogIndex()
	lastTerm := uint64(0)
	if entry := n.GetLog(lastIndex); entry != nil {
		lastTerm = entry.TermId 
	}

	// Checks the real field name and type
	req := &RequestVoteRequest{
		Term:         currTerm,
		Candidate:    n.Self, 
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	type reply struct {
		resp *RequestVoteReply
		err  error
	}

	var others []*RemoteNode
	for _, p := range n.getPeers() {
		if p.Id != n.Self.Id {
			others = append(others, p)
		}
	}

	replies := make(chan reply, len(others)) 
	for _, p := range others {
		go func(p *RemoteNode) {
			resp, err := p.RequestVoteRPC(n, req)
			replies <- reply{resp, err}
		}(p)
	}

	// Our own vote
	votes := 1 
	majority := (len(others)+1)/2 + 1

	// // For single node clusters
	// if votes >= majority { 
	// 	return false, true
	// }

	// Decide as soon as the outcome is known, instead of waiting for every peer.
	// An unreachable or partitioned peer can take the full RPC timeout (2s) to
	// answer, which is far longer than the election timeout.
	// If unreachable, that counts as "no"
	for received := 0; received < len(others); received++ {
		if votes >= majority {
			return false, true
		}
		if votes+(len(others)-received) < majority {
			return false, false // a majority is no longer possible
		}

		r := <-replies
		if r.err != nil || r.resp == nil {
			continue
		}
		if r.resp.Term > currTerm {
			// Only move forward: this election may already be stale.
			if r.resp.Term > n.GetCurrentTerm() {
				n.SetCurrentTerm(r.resp.Term)
				n.setVotedFor("")
			}
			return true, false
		}
		if r.resp.VoteGranted {
			votes++
		}
	}
	return false, votes >= majority
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	
	// Decides the vote and replies
	if msg.request.Term > n.GetCurrentTerm(){
		n.followerHandleRequestVote(msg)
		return true
	}

	// On equal and lesser terms, stay a candidate
	msg.reply <- RequestVoteReply{Term: n.GetCurrentTerm(), VoteGranted: false}
	return false
}
