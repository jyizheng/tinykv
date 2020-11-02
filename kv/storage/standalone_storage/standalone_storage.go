package standalone_storage

import (
	"io/ioutil"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines * engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dir, err := ioutil.TempDir("", "default")
	if err != nil {
		panic(err)
	}

	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)

	if err != nil {
		panic(err)
	}

	en := engine_util.NewEngines(db, nil, dir, dir)
	return &StandAloneStorage {
		engines:    en,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{s, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	en := s.engines
	var err error

	CfDefault := "default"
	CfLock := "lock"
	CfWrite := "write"

	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			switch data.Cf {
			case engine_util.CfDefault:
				err = engine_util.PutCF(en.Kv, CfDefault, data.Key, data.Value)
			case engine_util.CfLock:
				err = engine_util.PutCF(en.Kv, CfLock, data.Key, data.Value)
			case engine_util.CfWrite:
				err = engine_util.PutCF(en.Kv, CfWrite, data.Key, data.Value)
			}
		case storage.Delete:
			switch data.Cf {
			case engine_util.CfDefault:
				err = engine_util.DeleteCF(en.Kv, CfDefault, data.Key)
			case engine_util.CfLock:
				err = engine_util.DeleteCF(en.Kv, CfLock, data.Key)
			case engine_util.CfWrite:
				err = engine_util.DeleteCF(en.Kv, CfWrite, data.Key)
			}
		}
	}
	return err
}

type StandAloneReader struct {
	inner *StandAloneStorage
	iterCount int
}

func (sr *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var result []byte
	var err error

	switch cf {
	case engine_util.CfDefault:
		result, err = engine_util.GetCF(sr.inner.engines.Kv , cf, key)
	case engine_util.CfLock:
		result, err = engine_util.GetCF(sr.inner.engines.Kv , cf, key)
	case engine_util.CfWrite:
		result, err = engine_util.GetCF(sr.inner.engines.Kv , cf, key)
	default:
		return nil, fmt.Errorf("mem-server: bad CF %s", cf)
	}
	// For TestRawDelete1
	if err == badger.ErrKeyNotFound {
		if result != nil {
			panic(err)
		}
		err = nil
	}
	return result, err
}

func (sr *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	var data *badger.DB

	switch cf {
	case engine_util.CfDefault:
		data = sr.inner.engines.Kv
	case engine_util.CfLock:
		data = sr.inner.engines.Kv
	case engine_util.CfWrite:
		data = sr.inner.engines.Kv
	default:
		return nil
	}

	sr.iterCount += 1
	txn := data.NewTransaction(false)
	iter := engine_util.NewCFIterator(cf, txn)

	return &StandAloneIter{data, iter, txn, cf, sr, nil}
}

func (sr *StandAloneReader) Close() {
	if sr.iterCount > 0 {
		fmt.Println("sr.iterCount:", sr.iterCount)
		panic("Unclosed iterator")
	}
}

type StandAloneIter struct {
	data   *badger.DB
	iter   *engine_util.BadgerIterator
	txn    *badger.Txn
	cf     string
	reader *StandAloneReader
	item   engine_util.DBItem
}

func (it *StandAloneIter) Item() engine_util.DBItem {
	it.item = it.iter.Item()
	return it.item
}

func (it *StandAloneIter) Valid() bool {
	return it.iter.Valid()
}
func (it *StandAloneIter) Next() {
	it.iter.Next()
}

func (it *StandAloneIter) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *StandAloneIter) Start(data *badger.DB, cf string) {
	//it.txn = data.NewTransaction(false)
	//it.iter = NewCFIterator(cf, it.txn)
}

func (it *StandAloneIter) Close() {
	it.reader.iterCount -= 1
	it.iter.Close()
	it.txn.Discard()
}

