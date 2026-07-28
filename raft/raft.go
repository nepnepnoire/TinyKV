// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// randomizedElectionTimeout is chosen from
	// [electionTimeout, 2*electionTimeout).
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftLog := newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}
	prs := make(map[uint64]*Progress, len(peers))
	for _, id := range peers {
		prs[id] = &Progress{Next: raftLog.LastIndex() + 1}
	}
	raftLog.committed = hardState.Commit
	if raftLog.committed > raftLog.LastIndex() {
		panic("hard state commit is out of range")
	}
	if c.Applied != 0 {
		if c.Applied > raftLog.committed {
			panic("applied index is greater than committed index")
		}
		raftLog.applied = c.Applied
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	r.resetRandomizedElectionTimeout()
	for _, ent := range raftLog.entries {
		if ent.Index > raftLog.applied && ent.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = ent.Index
		}
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	if pr == nil {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		snapshot, snapErr := r.RaftLog.storage.Snapshot()
		if IsEmptySnap(&snapshot) && !IsEmptySnap(r.RaftLog.pendingSnapshot) {
			snapshot = *r.RaftLog.pendingSnapshot
			snapErr = nil
		}
		if snapErr == ErrSnapshotTemporarilyUnavailable || IsEmptySnap(&snapshot) {
			return false
		}
		if snapErr != nil {
			panic(snapErr)
		}
		r.send(pb.Message{
			To:       to,
			MsgType:  pb.MessageType_MsgSnapshot,
			Snapshot: &snapshot,
		})
		return true
	}
	entries, err := r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		panic(err)
	}
	entryPointers := make([]*pb.Entry, len(entries))
	for i := range entries {
		entry := entries[i]
		entryPointers[i] = &entry
	}
	r.send(pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgAppend,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entryPointers,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgHeartbeat})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		r.electionElapsed++
		if r.leadTransferee != None && r.electionElapsed >= r.electionTimeout {
			r.abortLeaderTransfer()
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition from leader to candidate")
	}
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		panic("invalid transition from follower to leader")
	}
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	last := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		pr.Match = 0
		pr.Next = last + 1
		if id == r.id {
			pr.Match = last
		}
	}
	r.appendEntry(pb.Entry{})
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		lead := None
		switch m.MsgType {
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	} else if m.Term != 0 && m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat:
			r.send(pb.Message{
				To:      m.From,
				MsgType: pb.MessageType_MsgAppendResponse,
				Index:   r.RaftLog.LastIndex(),
				Reject:  true,
			})
		}
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader && r.Prs[r.id] != nil {
			r.campaign()
		}
		return nil
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		upToDate := m.LogTerm > r.RaftLog.lastTerm() ||
			(m.LogTerm == r.RaftLog.lastTerm() && m.Index >= r.RaftLog.LastIndex())
		granted := canVote && upToDate
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  !granted,
		})
		if granted {
			r.electionElapsed = 0
			r.Vote = m.From
		}
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			if r.Lead == None {
				return ErrProposalDropped
			}
			m.To = r.Lead
			r.send(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead == None {
				return nil
			}
			m.To = r.Lead
			m.Term = r.Term
			r.msgs = append(r.msgs, m)
		case pb.MessageType_MsgTimeoutNow:
			if r.Prs[r.id] != nil {
				r.campaign()
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			return ErrProposalDropped
		case pb.MessageType_MsgAppend:
			r.becomeFollower(r.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(r.Term, m.From)
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.becomeFollower(r.Term, m.From)
			r.handleSnapshot(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.poll(m.From, !m.Reject)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				m.Term = r.Term
				r.msgs = append(r.msgs, m)
			}
		case pb.MessageType_MsgTimeoutNow:
			if r.Prs[r.id] != nil {
				r.campaign()
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.bcastHeartbeat()
		case pb.MessageType_MsgPropose:
			if len(m.Entries) == 0 || r.Prs[r.id] == nil || r.leadTransferee != None {
				return ErrProposalDropped
			}
			entries := make([]pb.Entry, 0, len(m.Entries))
			for _, entry := range m.Entries {
				if entry == nil {
					continue
				}
				copied := *entry
				if copied.EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex > r.RaftLog.applied {
						copied.EntryType = pb.EntryType_EntryNormal
						copied.Data = nil
					} else {
						r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(len(entries)) + 1
					}
				}
				entries = append(entries, copied)
			}
			if len(entries) == 0 {
				return ErrProposalDropped
			}
			r.appendEntry(entries...)
			r.bcastAppend()
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			r.becomeFollower(r.Term, m.From)
			if m.MsgType == pb.MessageType_MsgAppend {
				r.handleAppendEntries(m)
			} else if m.MsgType == pb.MessageType_MsgHeartbeat {
				r.handleHeartbeat(m)
			} else {
				r.handleSnapshot(m)
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			if pr := r.Prs[m.From]; pr != nil && pr.Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgTransferLeader:
			r.handleTransferLeader(m.From)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
		})
		return
	}
	if !r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}

	lastNewIndex := m.Index
	for i, entry := range m.Entries {
		if entry == nil {
			continue
		}
		lastNewIndex = entry.Index
		term, err := r.RaftLog.Term(entry.Index)
		if err == nil && term == entry.Term {
			continue
		}
		if entry.Index <= r.RaftLog.committed {
			panic("attempted to overwrite a committed raft entry")
		}
		newEntries := make([]pb.Entry, 0, len(m.Entries)-i)
		for _, incoming := range m.Entries[i:] {
			if incoming != nil {
				newEntries = append(newEntries, *incoming)
			}
		}
		r.RaftLog.truncateAndAppend(newEntries)
		r.recomputePendingConfIndex()
		break
	}
	if len(m.Entries) != 0 {
		lastNewIndex = m.Index + uint64(len(m.Entries))
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   lastNewIndex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	if IsEmptySnap(m.Snapshot) {
		return
	}
	snapshot := *m.Snapshot
	if snapshot.Metadata.Index <= r.RaftLog.committed {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.committed,
		})
		return
	}
	r.RaftLog.restore(snapshot)
	r.Prs = make(map[uint64]*Progress)
	if snapshot.Metadata.ConfState != nil {
		for _, id := range snapshot.Metadata.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}
	}
	r.PendingConfIndex = 0
	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   r.RaftLog.LastIndex(),
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if id == None {
		return
	}
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}
	r.PendingConfIndex = 0
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		r.PendingConfIndex = 0
		return
	}
	delete(r.Prs, id)
	r.PendingConfIndex = 0
	if r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
	if r.State == StateLeader && len(r.Prs) != 0 && r.maybeCommit() {
		r.bcastAppend()
	}
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.abortLeaderTransfer()
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.Term == 0 {
		switch m.MsgType {
		case pb.MessageType_MsgPropose, pb.MessageType_MsgHup:
		default:
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	r.votes[r.id] = true
	if r.quorum() == 1 {
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastTerm := r.RaftLog.lastTerm()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			To:      id,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   lastIndex,
			LogTerm: lastTerm,
		})
	}
}

func (r *Raft) poll(id uint64, granted bool) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = granted
	}
	grantedCount, rejectedCount := 0, 0
	for _, vote := range r.votes {
		if vote {
			grantedCount++
		} else {
			rejectedCount++
		}
	}
	if grantedCount >= r.quorum() {
		r.becomeLeader()
	} else if rejectedCount >= r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) appendEntry(entries ...pb.Entry) {
	last := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = last + uint64(i) + 1
	}
	r.RaftLog.truncateAndAppend(entries)
	if pr := r.Prs[r.id]; pr != nil {
		pr.Match = r.RaftLog.LastIndex()
		pr.Next = pr.Match + 1
	}
	r.maybeCommit()
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) maybeCommit() bool {
	if len(r.Prs) == 0 {
		return false
	}
	matches := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	index := matches[r.quorum()-1]
	if index <= r.RaftLog.committed {
		return false
	}
	term, err := r.RaftLog.Term(index)
	if err != nil || term != r.Term {
		return false
	}
	r.RaftLog.committed = index
	return true
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}
	if m.Reject {
		if pr.Next > 1 {
			pr.Next--
		}
		if m.Index+1 < pr.Next {
			pr.Next = m.Index + 1
		}
		r.sendAppend(m.From)
		return
	}
	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = m.Index + 1
	}
	if r.maybeCommit() {
		r.bcastAppend()
	} else if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
	if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgTimeoutNow})
	}
}

func (r *Raft) handleTransferLeader(transferee uint64) {
	pr := r.Prs[transferee]
	if pr == nil || transferee == r.id {
		return
	}
	if r.leadTransferee != transferee {
		r.electionElapsed = 0
		r.leadTransferee = transferee
	}
	if pr.Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{To: transferee, MsgType: pb.MessageType_MsgTimeoutNow})
	} else {
		r.sendAppend(transferee)
	}
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func (r *Raft) recomputePendingConfIndex() {
	r.PendingConfIndex = 0
	for _, entry := range r.RaftLog.entries {
		if entry.Index > r.RaftLog.applied && entry.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entry.Index
		}
	}
}
