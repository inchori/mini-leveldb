package db_test

import (
	"mini-leveldb/db"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBGetAndPut(t *testing.T) {
	store := db.NewDB()
	_ = store.Put("foo", "bar")

	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{"Get existing key", "foo", "bar", false},
		{"Get non-existing key", "baz", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Get(tt.key)
			if (err != nil) != tt.wantErr {
				assert.Error(t, err)
			}
			if got != tt.want {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
