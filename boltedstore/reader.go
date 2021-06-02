package boltedstore

import (
	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type Reader struct {
	tx   bolted.WriteTx
	path dbpath.Path
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	kp := r.path.Append(string(key))
	if !r.tx.Exists(kp) {
		return nil, nil
	}

	v := r.tx.Get(kp)
	return v, nil
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	it := r.tx.Iterator(r.path)

	rv := &PrefixIterator{
		it:      it,
		prefix:  string(prefix),
		isValid: true,
	}

	rv.Seek(prefix)
	return rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	it := r.tx.Iterator(r.path)

	rv := &RangeIterator{
		it:      it,
		start:   string(start),
		end:     string(end),
		isValid: true,
	}

	rv.Seek(start)
	return rv
}

func (r *Reader) Close() error {
	return nil
}
