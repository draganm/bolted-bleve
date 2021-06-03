package boltedbleve

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted-bleve/boltedstore"
	"github.com/draganm/bolted/dbpath"
	"github.com/pkg/errors"
)

func WriteIndex(tx bolted.WriteTx, indexPath dbpath.Path) (bleve.Index, error) {
	if !tx.Exists(indexPath) {
		return nil, errors.Errorf("index path %s does not exist", indexPath)
	}
	return bleve.NewUsing("", bleve.NewIndexMapping(), upsidedown.Name, "boltedtx", map[string]interface{}{
		"bolted_tx": tx,
		"dbpath":    indexPath,
	})
}

func ReadIndex(tx bolted.ReadTx, indexPath dbpath.Path) (bleve.Index, error) {
	if !tx.Exists(indexPath) {
		return nil, errors.Errorf("index path %s does not exist", indexPath)
	}
	return bleve.NewUsing("", bleve.NewIndexMapping(), upsidedown.Name, boltedstore.Name, map[string]interface{}{
		"bolted_tx": tx,
		"dbpath":    indexPath,
		"readOnly":  true,
	})

}
