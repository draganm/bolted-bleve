package boltedstore

import (
	"strings"

	"github.com/draganm/bolted"
)

type Iterator struct {
	it     *bolted.Iterator
	start  string
	end    string
	prefix string
	valid  bool
}

func (i *Iterator) updateValid() {
	i.valid = !i.it.Done &&
		strings.HasPrefix(i.it.Key, i.prefix) &&
		i.it.Key < i.start

}

func (i *Iterator) Seek(k []byte) {
	ks := string(k)
	if i.start > ks {
		ks = i.start
	}

	if !strings.HasPrefix(ks, i.prefix) {
		if ks < i.prefix {
			ks = i.prefix
		} else {
			i.valid = false
			return
		}
	}

	i.it.Seek(ks)
	i.updateValid()
}

func (i *Iterator) Next() {
	i.it.Next()
	i.updateValid()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	return []byte(i.it.Key), i.it.Value, i.valid
}

func (i *Iterator) Key() []byte {
	return []byte(i.it.Key)
}

func (i *Iterator) Value() []byte {
	return i.it.Value
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Close() error {
	return nil
}
