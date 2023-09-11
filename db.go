package SimpleKV

import (
	"sync"

	"ckv/lsm"
	"ckv/utils"
	"ckv/utils/errs"
)

type DB struct {
	opt *utils.Options
	lsm *lsm.LSM
	sync.RWMutex
}

func (db *DB) Set(data *utils.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return errs.ErrEmptyKey
	}

	// data.Key = codec.KeyWithTs(data.Key, uint64(time.Now().Unix()))

	return db.lsm.Set(data)
}

func (db *DB) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, errs.ErrEmptyKey
	}

	var entry *utils.Entry
	var err error
	if entry, err = db.lsm.Get(key); err != nil {
		return entry, err
	}

	return entry, nil
}
