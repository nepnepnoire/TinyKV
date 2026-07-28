package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := new(kvrpcpb.GetResponse)
	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := new(kvrpcpb.PrewriteResponse)
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	keys = uniqueKeys(keys)
	server.waitForLatches(keys)
	defer server.releaseLatches(keys)

	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		write, commitTS, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTS >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)})
			continue
		}
		if mutation.Op != kvrpcpb.Op_Put && mutation.Op != kvrpcpb.Op_Del {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Abort: fmt.Sprintf("unsupported mutation operation %s", mutation.Op),
			})
		}
	}
	if len(resp.Errors) != 0 {
		return resp, nil
	}

	for _, mutation := range req.Mutations {
		kind := mvcc.WriteKindFromProto(mutation.Op)
		if kind == mvcc.WriteKindPut {
			txn.PutValue(mutation.Key, mutation.Value)
		} else {
			txn.DeleteValue(mutation.Key)
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}
	server.Latches.Validate(txn, keys)
	regionErr, err = server.writeTransaction(req.Context, txn.Writes())
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := new(kvrpcpb.CommitResponse)
	keys := uniqueKeys(req.Keys)
	server.waitForLatches(keys)
	defer server.releaseLatches(keys)

	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if len(keys) > 0 && req.CommitVersion <= req.StartVersion {
		resp.Error = &kvrpcpb.KeyError{Abort: "commit timestamp must be greater than start timestamp"}
		return resp, nil
	}
	resp.Error, err = prepareCommit(txn, keys, req.CommitVersion)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return resp, nil
	}

	server.Latches.Validate(txn, keys)
	regionErr, err = server.writeTransaction(req.Context, txn.Writes())
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := new(kvrpcpb.ScanResponse)
	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for uint32(len(resp.Pairs)) < req.Limit {
		key, value, err := scanner.Next()
		if err != nil {
			keyErr, ok := err.(*mvcc.KeyError)
			if !ok {
				return nil, err
			}
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Error: &keyErr.KeyError,
			})
			continue
		}
		if key == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	keys := [][]byte{req.PrimaryKey}
	server.waitForLatches(keys)
	defer server.releaseLatches(keys)

	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTS
		}
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil || lock.Ts != req.LockTs {
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		resp.LockTtl = lock.Ttl
	}

	server.Latches.Validate(txn, keys)
	regionErr, err = server.writeTransaction(req.Context, txn.Writes())
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := new(kvrpcpb.BatchRollbackResponse)
	keys := uniqueKeys(req.Keys)
	server.waitForLatches(keys)
	defer server.releaseLatches(keys)

	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp.Error, err = prepareRollback(txn, keys)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return resp, nil
	}

	server.Latches.Validate(txn, keys)
	regionErr, err = server.writeTransaction(req.Context, txn.Writes())
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, regionErr, err := server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	scanTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(scanTxn)
	reader.Close()
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, len(locks))
	for _, pair := range locks {
		keys = append(keys, pair.Key)
	}
	keys = uniqueKeys(keys)
	if len(keys) == 0 {
		return resp, nil
	}
	server.waitForLatches(keys)
	defer server.releaseLatches(keys)

	reader, regionErr, err = server.transactionReader(req.Context)
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if req.CommitVersion == 0 {
		resp.Error, err = prepareRollback(txn, keys)
	} else if req.CommitVersion <= req.StartVersion {
		resp.Error = &kvrpcpb.KeyError{Abort: "commit timestamp must be greater than start timestamp"}
	} else {
		resp.Error, err = prepareCommit(txn, keys, req.CommitVersion)
	}
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return resp, nil
	}

	server.Latches.Validate(txn, keys)
	regionErr, err = server.writeTransaction(req.Context, txn.Writes())
	if regionErr != nil {
		resp.RegionError = regionErr
		return resp, nil
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func prepareCommit(txn *mvcc.MvccTxn, keys [][]byte, commitTS uint64) (*kvrpcpb.KeyError, error) {
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.KeyError{Abort: "transaction has already been rolled back"}, nil
			}
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			continue
		}
		if lock.Ts != txn.StartTS {
			return &kvrpcpb.KeyError{Retryable: "key is locked by another transaction"}, nil
		}
		if lock.Kind == mvcc.WriteKindRollback {
			return &kvrpcpb.KeyError{Abort: "cannot commit a rollback lock"}, nil
		}
		txn.PutWrite(key, commitTS, &mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind})
		txn.DeleteLock(key)
	}
	return nil, nil
}

func prepareRollback(txn *mvcc.MvccTxn, keys [][]byte) (*kvrpcpb.KeyError, error) {
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.KeyError{Abort: "transaction has already been committed"}, nil
			}
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == txn.StartTS {
			txn.DeleteLock(key)
		}
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	return nil, nil
}

func (server *Server) transactionReader(ctx *kvrpcpb.Context) (storage.StorageReader, *errorpb.Error, error) {
	reader, err := server.storage.Reader(ctx)
	if err == nil {
		return reader, nil, nil
	}
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		return nil, regionErr.RequestErr, nil
	}
	return nil, nil, err
}

func (server *Server) writeTransaction(ctx *kvrpcpb.Context, writes []storage.Modify) (*errorpb.Error, error) {
	if len(writes) == 0 {
		return nil, nil
	}
	err := server.storage.Write(ctx, writes)
	if err == nil {
		return nil, nil
	}
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		return regionErr.RequestErr, nil
	}
	return nil, err
}

func (server *Server) waitForLatches(keys [][]byte) {
	if len(keys) != 0 {
		server.Latches.WaitForLatches(keys)
	}
}

func (server *Server) releaseLatches(keys [][]byte) {
	if len(keys) != 0 {
		server.Latches.ReleaseLatches(keys)
	}
}

func uniqueKeys(keys [][]byte) [][]byte {
	result := make([][]byte, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := seen[string(key)]; ok {
			continue
		}
		seen[string(key)] = struct{}{}
		result = append(result, key)
	}
	return result
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
