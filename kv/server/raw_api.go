package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		setRawGetError(resp, err)
		return resp, nil
	}
	defer reader.Close()

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		setRawGetError(resp, err)
		return resp, nil
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}})
	if err != nil {
		setRawPutError(resp, err)
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(req.Context, []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}})
	if err != nil {
		setRawDeleteError(resp, err)
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		setRawScanError(resp, err)
		return resp, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	for iter.Seek(req.StartKey); iter.Valid() && uint32(len(resp.Kvs)) < req.Limit; iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, nil
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: value,
		})
	}
	return resp, nil
}

func setRawGetError(resp *kvrpcpb.RawGetResponse, err error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
	} else {
		resp.Error = err.Error()
	}
}

func setRawPutError(resp *kvrpcpb.RawPutResponse, err error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
	} else {
		resp.Error = err.Error()
	}
}

func setRawDeleteError(resp *kvrpcpb.RawDeleteResponse, err error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
	} else {
		resp.Error = err.Error()
	}
}

func setRawScanError(resp *kvrpcpb.RawScanResponse, err error) {
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
	} else {
		resp.Error = err.Error()
	}
}
