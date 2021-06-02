package boltedstore

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/v2/registry"
	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type Store struct {
	tx          bolted.WriteTx
	dbpath      dbpath.Path
	fillPercent float64
	mo          store.MergeOperator
	readOnly    bool
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	tx, ok := config["bolted_tx"].(bolted.WriteTx)
	if !ok || tx == nil {
		return nil, fmt.Errorf("must specify bolted tx")
	}

	dbpath, ok := config["dbpath"].(dbpath.Path)
	if !ok {

		return nil, fmt.Errorf("must specify dbpath")
	}

	readOnly, ok := config["readOnly"].(bool)
	if !ok {
		readOnly = false
	}

	rv := Store{
		dbpath:   dbpath,
		tx:       tx,
		mo:       mo,
		readOnly: readOnly,
	}
	return &rv, nil
}

func (bs *Store) Close() error {
	return nil
}

func (bs *Store) Reader() (store.KVReader, error) {

	return &Reader{
		tx:   bs.tx,
		path: bs.dbpath,
	}, nil
}

func (bs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		tx:       bs.tx,
		path:     bs.dbpath,
		mo:       bs.mo,
		readOnly: bs.readOnly,
	}, nil
}

func (bs *Store) Stats() json.Marshaler {
	return &stats{
		s: bs,
	}
}

func (bs *Store) CompactWithBatchSize(batchSize int) error {
	return bs.Compact()
}

func (bs *Store) Compact() error {
	toDelete := []dbpath.Path{}
	for it := bs.tx.Iterator(bs.dbpath); !it.Done; it.Next() {
		if bytes.Equal(it.Value, []byte{0}) {
			toDelete = append(toDelete, bs.dbpath.Append(it.Key))
		}
	}

	for _, p := range toDelete {
		bs.tx.Delete(p)
	}

	return nil
}

func init() {
	registry.RegisterKVStore("boltedtx", New)
}

const Name = "boltedtx"
