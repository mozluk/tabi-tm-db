package backends

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/syndtr/goleveldb/leveldb"
	dbm "github.com/tendermint/tm-db"
)

const (
	IndexKeyPrefixLen = 128
	Sha256Base64Len   = 44
	IndexEntryLen     = IndexKeyPrefixLen + Sha256Base64Len
)

type IndexEntry struct {
	keyPrefix string
	txId      []byte
}

// NewIndexEntryFromBytes parses one fixed-size index entry. It returns an
// error instead of panicking when the index bytes are shorter than the
// fixed entry size, since index data comes from the (untrusted) Arweave
// network.
func NewIndexEntryFromBytes(bz []byte) (IndexEntry, error) {
	if len(bz) < IndexEntryLen {
		return IndexEntry{}, fmt.Errorf(
			"index entry too short: got %d bytes, need at least %d",
			len(bz), IndexEntryLen,
		)
	}
	keyPrefix := string(bz[:IndexKeyPrefixLen])
	return IndexEntry{
		keyPrefix: keyPrefix,
		txId:      bz[IndexKeyPrefixLen:IndexEntryLen],
	}, nil
}

// A read-only backend that stores data on Arweave. Each key being
// queried needs to be prefixed with 8 bytes indicating the version
// to query for, from an uint64 encoded in big endian format.
// A query is processed in 3 steps:
// 1. Get the Arweave transaction ID which stores the queried
//    version's index from a local leveldb
// 2. Query Arweave for the queried version's index and get the
//    transaction ID(s) which store the actual queried data
// 3. Query Arweave with the transaction ID(s) from 2 and find
//    the queried value.
//
// To use an iterator, both `start` and `end` need to have to same
// version prefix.
type ArweaveDB struct {
	txDataByIdGetter  func([]byte) ([]byte, error)
	versionTxIdGetter func([]byte) ([]byte, error)
	closer            func() error
}

var _ dbm.DB = (*ArweaveDB)(nil)

func NewArweaveDB(indexDBFullPath string, arweaveNodeURL string) (*ArweaveDB, error) {
	indexDB, err := leveldb.OpenFile(indexDBFullPath, nil)
	if err != nil {
		return nil, err
	}
	arweaveClient, err := NewClient(arweaveNodeURL)
	if err != nil {
		_ = indexDB.Close()
		return nil, err
	}
	return &ArweaveDB{
		txDataByIdGetter: func(txId []byte) ([]byte, error) {
			return arweaveClient.DownloadChunkData(string(txId))
		},
		versionTxIdGetter: func(version []byte) ([]byte, error) {
			return indexDB.Get(version, nil)
		},
		closer: func() error {
			return indexDB.Close()
		},
	}, nil
}

func NewEmptyArweaveDB() *ArweaveDB {
	return &ArweaveDB{}
}

// Get implements DB.
func (db *ArweaveDB) Get(key []byte) ([]byte, error) {
	if len(key) < 8 {
		return nil, fmt.Errorf("key too short for ArweaveDB: %d bytes (need 8-byte version prefix)", len(key))
	}
	txIds, err := db.getArweaveTxIds(key)
	if err != nil {
		return nil, err
	}
	return db.getKeyByTxIds(key, txIds)
}

// Has implements DB.
func (db *ArweaveDB) Has(key []byte) (bool, error) {
	if len(key) < 8 {
		return false, fmt.Errorf("key too short for ArweaveDB: %d bytes (need 8-byte version prefix)", len(key))
	}
	txIds, err := db.getArweaveTxIds(key)
	if err == nil {
		_, err = db.getKeyByTxIds(key, txIds)
	}
	if err != nil {
		if _, ok := err.(*ErrKeyNotFound); ok {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Set implements DB.
func (db *ArweaveDB) Set(key []byte, value []byte) error {
	panic("Arweave backend is read-only")
}

// SetSync implements DB.
func (db *ArweaveDB) SetSync(key []byte, value []byte) error {
	panic("Arweave backend is read-only")
}

// Delete implements DB.
func (db *ArweaveDB) Delete(key []byte) error {
	panic("Arweave backend is read-only")
}

// DeleteSync implements DB.
func (db *ArweaveDB) DeleteSync(key []byte) error {
	panic("Arweave backend is read-only")
}

// Close implements DB.
func (db *ArweaveDB) Close() error {
	if db.closer == nil {
		// Nothing to close for empty/handle-less instances (e.g. tests).
		return nil
	}
	return db.closer()
}

// Print implements DB.
func (db *ArweaveDB) Print() error {
	return nil
}

// Stats implements DB.
func (db *ArweaveDB) Stats() map[string]string {
	stats := make(map[string]string)
	return stats
}

// NewBatch implements DB.
func (db *ArweaveDB) NewBatch() dbm.Batch {
	panic("Arweave backend is read-only")
}

// Iterator implements DB.
func (db *ArweaveDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	return newArweaveDBIterator(start, end, db, false)
}

// ReverseIterator implements DB.
func (db *ArweaveDB) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return newArweaveDBIterator(start, end, db, true)
}

func (db *ArweaveDB) getKeyByTxIds(key []byte, txIds [][]byte) ([]byte, error) {
	key = key[8:]
	for _, txId := range txIds {
		keyvalues, err := db.getTxDataAsMap(txId)
		if err != nil {
			return nil, err
		}
		if val, ok := keyvalues[string(key)]; ok {
			// Use the comma-ok form: remote tx data is untrusted JSON and a
			// non-string value must not panic the host process.
			valueStr, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("tx data value for key %q is not a string (got %T)", string(key), val)
			}
			return []byte(valueStr), nil
		}
	}
	return nil, &ErrKeyNotFound{string(key)}
}

func (db *ArweaveDB) getTxDataAsMap(txId []byte) (map[string]interface{}, error) {
	txData, err := db.txDataByIdGetter(txId)
	if err != nil {
		return nil, err
	}
	keyvalues := map[string]interface{}{}
	if err := json.Unmarshal(txData, &keyvalues); err != nil {
		return nil, err
	}
	return keyvalues, nil
}

// First 8 bytes of key is version in big endian.
// Since we take a constant sized (128 bytes) prefix as range in
// the index, it's possible for some hot prefixes to have multiple
// entries in the index, so we need to be able to return multiple
// Tx IDs here.
func (db *ArweaveDB) getArweaveTxIds(key []byte) ([][]byte, error) {
	version := key[:8]
	index, err := db.getIndex(version)
	if err != nil {
		return nil, err
	}
	keyString := string(key[8:])
	entries, err := getIndexEntries(keyString, index)
	if err != nil {
		return nil, err
	}
	res := [][]byte{}
	for _, entry := range entries {
		res = append(res, entry.txId)
	}
	return res, nil
}

func (db *ArweaveDB) getIndex(version []byte) ([]byte, error) {
	indexTxId, err := db.versionTxIdGetter(version)
	if err != nil {
		return nil, err
	}
	return db.txDataByIdGetter(indexTxId)
}

// TODO: change to binary search
func getIndexEntries(keyString string, index []byte) ([]IndexEntry, error) {
	res := []IndexEntry{}
	for i := 0; i < len(index); i += IndexEntryLen {
		// Fail cleanly on truncated/malicious index data instead of slicing
		// out of bounds.
		if i+IndexEntryLen > len(index) {
			return nil, fmt.Errorf("index data truncated at entry %d: %d bytes left", i/IndexEntryLen, len(index)-i)
		}
		indexEntry, err := NewIndexEntryFromBytes(index[i : i+IndexEntryLen])
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			if res[0].keyPrefix == indexEntry.keyPrefix {
				res = append(res, indexEntry)
			} else {
				break
			}
		} else if keyString <= indexEntry.keyPrefix {
			res = append(res, indexEntry)
		}
	}
	return res, nil
}

func getIndexEntriesForRange(keyStart string, keyEnd string, index []byte) ([]IndexEntry, error) {
	res := []IndexEntry{}
	reachedEnd := false
	for i := 0; i < len(index); i += IndexEntryLen {
		// Fail cleanly on truncated/malicious index data instead of slicing
		// out of bounds.
		if i+IndexEntryLen > len(index) {
			return nil, fmt.Errorf("index data truncated at entry %d: %d bytes left", i/IndexEntryLen, len(index)-i)
		}
		indexEntry, err := NewIndexEntryFromBytes(index[i : i+IndexEntryLen])
		if err != nil {
			return nil, err
		}
		if reachedEnd {
			if res[len(res)-1].keyPrefix == indexEntry.keyPrefix {
				res = append(res, indexEntry)
			} else {
				break
			}
		} else if keyStart <= indexEntry.keyPrefix {
			res = append(res, indexEntry)
		}
		if keyEnd <= indexEntry.keyPrefix {
			reachedEnd = true
		}
	}
	return res, nil
}

type arweaveDBIterator struct {
	db      *ArweaveDB
	reverse bool

	start []byte
	end   []byte

	txIds             [][]byte
	currentTxData     map[string]interface{}
	currentSortedKeys []string
	currentKeyIdx     int
	txIdx             int

	finished bool
	// lastErr records the most recent failure (e.g. a network error while
	// loading the next transaction) so iterators report it via Error()
	// instead of panicking inside Next().
	lastErr error
}

var _ dbm.Iterator = (*arweaveDBIterator)(nil)

func newArweaveDBIterator(start []byte, end []byte, db *ArweaveDB, reverse bool) (*arweaveDBIterator, error) {
	// Guard against short keys before the version-equality slices below.
	if len(start) < 8 || len(end) < 8 {
		return nil, errors.New("start and end keys must include an 8-byte version prefix")
	}
	if string(start[:8]) != string(end[:8]) {
		return nil, errors.New("Start and end must be of the same version")
	}
	version := start[:8]
	index, err := db.getIndex(version)
	if err != nil {
		return nil, err
	}
	start, end = start[8:], end[8:]
	entries, err := getIndexEntriesForRange(string(start), string(end), index)
	if err != nil {
		return nil, err
	}
	txIds := [][]byte{}
	for _, entry := range entries {
		txIds = append(txIds, entry.txId)
	}
	txIdx := 0
	if reverse {
		txIdx = len(txIds) - 1
	}
	iter := &arweaveDBIterator{
		db:      db,
		reverse: reverse,
		start:   start,
		end:     end,
		txIds:   txIds,
		txIdx:   txIdx,
	}
	if err := iter.loadTx(); err != nil {
		return nil, err
	}
	if reverse {
		for string(iter.Key()) >= string(end) {
			iter.Next()
		}
	} else {
		for string(iter.Key()) < string(start) {
			iter.Next()
		}
	}
	if iter.lastErr != nil {
		// Surface errors encountered while skipping out-of-range entries.
		return nil, iter.lastErr
	}
	return iter, nil
}

func (itr *arweaveDBIterator) loadTx() error {
	if itr.finished {
		return nil
	}
	if itr.txIdx >= len(itr.txIds) || itr.txIdx < 0 {
		itr.finished = true
		return nil
	}
	data, err := itr.db.getTxDataAsMap(itr.txIds[itr.txIdx])
	if err != nil {
		// Record the failure instead of propagating it into a panic from
		// callers such as Next(); the iterator becomes invalid and reports
		// the cause via Error().
		itr.lastErr = err
		itr.finished = true
		return nil
	}
	if len(data) == 0 {
		if itr.reverse {
			itr.txIdx--
		} else {
			itr.txIdx++
		}
		return itr.loadTx()
	}
	itr.currentTxData = data
	itr.currentSortedKeys = make([]string, len(itr.currentTxData))
	i := 0
	for k := range itr.currentTxData {
		itr.currentSortedKeys[i] = k
		i++
	}
	sort.Strings(itr.currentSortedKeys)
	if itr.reverse {
		itr.currentKeyIdx = len(itr.currentSortedKeys) - 1
		if string(itr.Key()) < string(itr.start) {
			itr.finished = true
		}
	} else {
		itr.currentKeyIdx = 0
		if string(itr.Key()) >= string(itr.end) {
			itr.finished = true
		}
	}
	return nil
}

// Domain implements Iterator.
func (itr *arweaveDBIterator) Domain() ([]byte, []byte) {
	return []byte{}, []byte{}
}

// Valid implements Iterator.
func (itr *arweaveDBIterator) Valid() bool {
	if itr.lastErr != nil {
		return false
	}
	return !itr.finished
}

// Key implements Iterator.
func (itr *arweaveDBIterator) Key() []byte {
	if itr.finished {
		return nil
	}
	return []byte(itr.currentSortedKeys[itr.currentKeyIdx])
}

// Value implements Iterator.
func (itr *arweaveDBIterator) Value() []byte {
	if itr.finished {
		return nil
	}
	// Comma-ok form: remote tx data is untrusted JSON; a non-string value
	// must not crash the host process.
	raw := itr.currentTxData[itr.currentSortedKeys[itr.currentKeyIdx]]
	valueStr, ok := raw.(string)
	if !ok {
		return nil
	}
	return []byte(valueStr)
}

// Next implements Iterator.
func (itr *arweaveDBIterator) Next() {
	if !itr.Valid() {
		return
	}
	if itr.reverse {
		if itr.currentKeyIdx > 0 {
			itr.currentKeyIdx--
			if string(itr.Key()) < string(itr.start) {
				itr.finished = true
			}
		} else {
			itr.txIdx--
			// On failure loadTx records lastErr and marks the iterator
			// invalid; do not panic the host process on transient errors.
			if err := itr.loadTx(); err != nil {
				itr.lastErr = err
				itr.finished = true
			}
		}
	} else {
		if itr.currentKeyIdx < len(itr.currentSortedKeys)-1 {
			itr.currentKeyIdx++
			if string(itr.Key()) >= string(itr.end) {
				itr.finished = true
			}
		} else {
			itr.txIdx++
			// On failure loadTx records lastErr and marks the iterator
			// invalid; do not panic the host process on transient errors.
			if err := itr.loadTx(); err != nil {
				itr.lastErr = err
				itr.finished = true
			}
		}
	}
}

// Error implements Iterator.
func (itr *arweaveDBIterator) Error() error {
	return itr.lastErr
}

// Close implements Iterator.
func (itr *arweaveDBIterator) Close() error {
	return nil
}
