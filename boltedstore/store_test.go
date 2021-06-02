package boltedstore_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/draganm/bolted"
	"github.com/draganm/bolted-bleve/boltedstore"
	"github.com/draganm/bolted/dbpath"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) (*bolted.Bolted, func()) {
	td, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	b, err := bolted.Open(filepath.Join(td, "db"), 0700)
	require.NoError(t, err)

	return b, func() {
		b.Close()
		os.RemoveAll(td)
	}
}

func TestIndex(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()

	message := struct {
		ID   string
		From string
		Body string
	}{
		ID:   "example",
		From: "marty.schoch@gmail.com",
		Body: "bleve indexing is easy",
	}

	indexPath := dbpath.ToPath("bleve")

	err := db.Write(func(tx bolted.WriteTx) error {
		tx.CreateMap(indexPath)

		ind, err := bleve.NewUsing("", bleve.NewIndexMapping(), upsidedown.Name, "boltedtx", map[string]interface{}{
			"bolted_tx": tx,
			"dbpath":    indexPath,
		})

		if err != nil {
			return errors.Wrap(err, "while creating index")
		}

		err = ind.Index(message.ID, message)
		if err != nil {
			return errors.Wrap(err, "while indexing")
		}
		return ind.Close()
	})

	require.NoError(t, err)

	var result *bleve.SearchResult

	err = db.Read(func(tx bolted.ReadTx) error {

		ind, err := bleve.NewUsing("", bleve.NewIndexMapping(), upsidedown.Name, boltedstore.Name, map[string]interface{}{
			"bolted_tx": tx,
			"dbpath":    indexPath,
			"readOnly":  true,
		})

		if err != nil {
			return errors.Wrap(err, "while creating index")
		}

		sq := bleve.NewMatchQuery("example")
		sr := bleve.NewSearchRequest(sq)
		result, err = ind.Search(sr)
		if err != nil {
			return errors.Wrap(err, "wile searching")
		}

		return nil

	})

	require.NoError(t, err)

	require.Len(t, result.Hits, 1)

}
