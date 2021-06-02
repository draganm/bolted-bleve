//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package boltedstore

import (
	"fmt"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted/dbpath"
)

type Writer struct {
	tx       bolted.WriteTx
	path     dbpath.Path
	mo       store.MergeOperator
	readOnly bool
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) (err error) {
	if w.readOnly {
		return nil
	}

	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		kb := []byte(k)
		pth := w.path.Append(k)
		existingVal := []byte{}
		if w.tx.Exists(pth) {
			existingVal = w.tx.Get(pth)
		}

		mergedVal, fullMergeOk := w.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			err = fmt.Errorf("merge operator returned failure")
			return
		}
		w.tx.Put(pth, mergedVal)
	}

	for _, op := range emulatedBatch.Ops {
		kp := w.path.Append(string(op.K))
		if op.V != nil {
			w.tx.Put(kp, op.V)
		} else {
			w.tx.Delete(kp)
		}
	}
	return
}

func (w *Writer) Close() error {
	return nil
}
