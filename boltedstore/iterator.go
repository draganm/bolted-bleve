package boltedstore

import (
	"strings"

	"github.com/draganm/bolted"
)

type PrefixIterator struct {
	it      *bolted.Iterator
	prefix  string
	isValid bool
}

func (i *PrefixIterator) Seek(k []byte) {
	ks := string(k)
	if !strings.HasPrefix(ks, i.prefix) {
		if ks < i.prefix {
			ks = i.prefix
		} else {
			i.isValid = false
			return
		}
	}
	i.it.Seek(ks)
	if i.it.Done {
		i.isValid = false
		return
	}
	if !strings.HasPrefix(i.it.Key, i.prefix) {
		i.isValid = false
		return
	}
	i.isValid = true
}

func (i *PrefixIterator) Next() {
	i.it.Next()
	if i.it.Done {
		i.isValid = false
		return
	}
	if !strings.HasPrefix(i.it.Key, i.prefix) {
		i.isValid = false
		return
	}
	i.isValid = true
}

func (i *PrefixIterator) Current() ([]byte, []byte, bool) {
	return []byte(i.it.Key), i.it.Value, i.isValid
}

func (i *PrefixIterator) Key() []byte {
	return []byte(i.it.Key)
}

func (i *PrefixIterator) Value() []byte {
	return i.it.Value
}

func (i *PrefixIterator) Valid() bool {
	return i.isValid
}

func (i *PrefixIterator) Close() error {
	return nil
}

type RangeIterator struct {
	it      *bolted.Iterator
	start   string
	end     string
	isValid bool
}

func (i *RangeIterator) Seek(k []byte) {
	ks := string(k)
	if ks < i.start {
		ks = i.start
	}

	i.it.Seek(ks)

	if i.it.Done {
		i.isValid = false
		return
	}
	if i.it.Key >= i.end {
		i.isValid = false
		return
	}
	i.isValid = true
}

func (i *RangeIterator) Next() {
	i.it.Next()

	if i.it.Done {
		i.isValid = false
		return
	}
	if i.it.Key >= i.end {
		i.isValid = false
		return
	}
	i.isValid = true
}

func (i *RangeIterator) Current() ([]byte, []byte, bool) {
	return []byte(i.it.Key), i.it.Value, i.isValid
}

func (i *RangeIterator) Key() []byte {
	return []byte(i.it.Key)
}

func (i *RangeIterator) Value() []byte {
	return i.it.Value
}

func (i *RangeIterator) Valid() bool {
	return i.isValid
}

func (i *RangeIterator) Close() error {
	return nil
}
