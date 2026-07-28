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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// entries starts at firstIndex. The entry immediately before it is kept
	// separately so AppendEntries can still compare terms at the compaction
	// boundary.
	firstIndex uint64
	dummyIndex uint64
	dummyTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	first, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	term, err := storage.Term(first - 1)
	if err != nil {
		panic(err)
	}

	entries := make([]pb.Entry, 0)
	if first <= last {
		entries, err = storage.Entries(first, last+1)
		if err != nil {
			panic(err)
		}
		entries = append([]pb.Entry(nil), entries...)
	}
	return &RaftLog{
		storage:    storage,
		committed:  first - 1,
		applied:    first - 1,
		stabled:    last,
		entries:    entries,
		firstIndex: first,
		dummyIndex: first - 1,
		dummyTerm:  term,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	first, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	if first <= l.firstIndex {
		return
	}

	dummy := first - 1
	term, err := l.Term(dummy)
	if err != nil {
		term, err = l.storage.Term(dummy)
		if err != nil {
			panic(err)
		}
	}
	if first <= l.LastIndex() {
		offset := first - l.firstIndex
		l.entries = append([]pb.Entry(nil), l.entries[offset:]...)
	} else {
		l.entries = nil
	}
	l.firstIndex = first
	l.dummyIndex = dummy
	l.dummyTerm = term
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	first := max(l.stabled+1, l.firstIndex)
	if first > l.LastIndex() {
		if l.entries != nil {
			return l.entries[len(l.entries):]
		}
		return nil
	}
	return l.entries[first-l.firstIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	first := max(l.applied+1, l.firstIndex)
	if first > l.committed {
		return nil
	}
	return l.entries[first-l.firstIndex : l.committed-l.firstIndex+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	last := l.dummyIndex
	if len(l.entries) != 0 {
		last = l.entries[len(l.entries)-1].Index
	}
	if !IsEmptySnap(l.pendingSnapshot) && l.pendingSnapshot.Metadata.Index > last {
		last = l.pendingSnapshot.Metadata.Index
	}
	return last
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}
	if i == l.dummyIndex {
		return l.dummyTerm, nil
	}
	if i < l.firstIndex {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.firstIndex].Term, nil
}

func (l *RaftLog) lastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	actual, err := l.Term(index)
	return err == nil && actual == term
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		panic("invalid raft log slice")
	}
	if lo < l.firstIndex {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	return l.entries[lo-l.firstIndex : hi-l.firstIndex], nil
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	if len(ents) == 0 {
		return
	}
	after := ents[0].Index
	switch {
	case after == l.LastIndex()+1:
		l.entries = append(l.entries, ents...)
	case after <= l.firstIndex:
		l.firstIndex = after
		l.entries = append([]pb.Entry(nil), ents...)
	default:
		prefix := append([]pb.Entry(nil), l.entries[:after-l.firstIndex]...)
		l.entries = append(prefix, ents...)
	}
	l.stabled = min(l.stabled, after-1)
}

func (l *RaftLog) restore(snapshot pb.Snapshot) {
	l.committed = snapshot.Metadata.Index
	l.applied = snapshot.Metadata.Index
	l.stabled = snapshot.Metadata.Index
	l.entries = nil
	l.firstIndex = snapshot.Metadata.Index + 1
	l.dummyIndex = snapshot.Metadata.Index
	l.dummyTerm = snapshot.Metadata.Term
	l.pendingSnapshot = &snapshot
}
