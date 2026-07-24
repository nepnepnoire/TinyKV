package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &standAloneReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			switch data := modify.Data.(type) {
			case storage.Put:
				if err := txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key)); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported storage modification %T", modify.Data)
			}
		}
		return nil
	})
}

type standAloneReader struct {
	txn *badger.Txn
}

func (r *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (r *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *standAloneReader) Close() {
	r.txn.Discard()
}
