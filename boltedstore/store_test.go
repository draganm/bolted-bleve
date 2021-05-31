package boltedstore_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/bolted"
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
	_, cleanup := openTestDB()
	defer cleanup()

}
