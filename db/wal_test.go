package db_test

import (
	"mini-leveldb/db"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWALAppendAndReplay(t *testing.T) {
	dir := "testdata/wal"
	_ = os.RemoveAll(dir)

	wal, err := db.NewWAL(dir)
	assert.NoError(t, err)
	defer wal.Close()

	t.Cleanup(func() {
		wal.Close()
		os.RemoveAll(dir)
		os.RemoveAll("testdata")
	})

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{"Append valid entry 1", "key1", "value1", false},
		{"Append valid entry 2", "key2", "value2", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wal.Append(tt.key, tt.value)
			if tt.wantErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.NoError(t, err, tt.name)
			}
		})
	}

	result, err := db.Replay(dir)
	assert.NoError(t, err)

	for _, tt := range tests {
		got := result[tt.key]
		assert.Equalf(t, tt.value, got, "Replay should return the correct value for key %s", tt.key)
	}
}
