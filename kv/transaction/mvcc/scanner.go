package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn       *MvccTxn
	writeIter engine_util.DBIterator
	lockIter  engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	lockIter := txn.Reader.IterCF(engine_util.CfLock)
	writeIter.Seek(EncodeKey(startKey, TsMax))
	lockIter.Seek(startKey)
	return &Scanner{
		txn:       txn,
		writeIter: writeIter,
		lockIter:  lockIter,
	}
}

func (scan *Scanner) Close() {
	scan.writeIter.Close()
	scan.lockIter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for scan.writeIter.Valid() || scan.lockIter.Valid() {
		key := scan.nextKey()
		scan.advancePast(key)

		lock, err := scan.txn.GetLock(key)
		if err != nil {
			return nil, nil, err
		}
		if lock != nil && lock.Ts <= scan.txn.StartTS &&
			(scan.txn.StartTS != TsMax || bytes.Equal(key, lock.Primary)) {
			return key, nil, &KeyError{KeyError: kvrpcpb.KeyError{Locked: lock.Info(key)}}
		}

		value, err := scan.txn.GetValue(key)
		if err != nil {
			return nil, nil, err
		}
		if value != nil {
			return key, value, nil
		}
	}
	return nil, nil, nil
}

func (scan *Scanner) nextKey() []byte {
	if !scan.writeIter.Valid() {
		return scan.lockIter.Item().KeyCopy(nil)
	}
	writeKey := DecodeUserKey(scan.writeIter.Item().KeyCopy(nil))
	if !scan.lockIter.Valid() {
		return writeKey
	}
	lockKey := scan.lockIter.Item().KeyCopy(nil)
	if bytes.Compare(writeKey, lockKey) <= 0 {
		return writeKey
	}
	return lockKey
}

func (scan *Scanner) advancePast(key []byte) {
	for scan.writeIter.Valid() {
		writeKey := DecodeUserKey(scan.writeIter.Item().KeyCopy(nil))
		if !bytes.Equal(writeKey, key) {
			break
		}
		scan.writeIter.Next()
	}
	if scan.lockIter.Valid() && bytes.Equal(scan.lockIter.Item().Key(), key) {
		scan.lockIter.Next()
	}
}
