package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type DB struct {
	memTable map[string]string
	wal      *WAL
	sstables []*SSTable
	dir      string
}

func NewDB(dir string) (*DB, error) {
	memTable, err := Replay(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to replay log: %w", err)
	}

	wal, err := NewWAL(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	db := &DB{
		memTable: memTable,
		wal:      wal,
		dir:      dir,
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, fmt.Errorf("failed to scan SSTable files: %w", err)
	}
	sort.Strings(files)

	for _, f := range files {
		sst := &SSTable{path: f}
		if err := sst.Load(); err != nil {
			// Skip corrupted or invalid SSTable and log the error
			log.Printf("Skipping SSTable %s due to load error: %v", f, err)
			continue
		}
		db.sstables = append(db.sstables, sst)
	}

	return db, nil
}

func (db *DB) Get(key string) (string, error) {
	if value, ok := db.memTable[key]; ok {
		return value, nil
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]
		if sst == nil || len(sst.index) == 0 {
			continue
		}

		if value, ok := db.sstables[i].BinarySearch(key); ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("failed to get key %s: not found", key)
}

func (db *DB) Put(key, value string) error {
	if key == "" {
		return fmt.Errorf("failed to put key %s: key cannot be empty", key)
	}

	if err := db.wal.Append(key, value); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	db.memTable[key] = value
	return nil
}

func (db *DB) Flush() error {
	if len(db.memTable) == 0 {
		return nil
	}

	kvs := make([][2]string, 0, len(db.memTable))
	keys := make([]string, 0, len(db.memTable))
	for k := range db.memTable {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kvs = append(kvs, [2]string{k, db.memTable[k]})
	}

	filename := fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano())
	sstablePath := filepath.Join(db.dir, filename)
	tmpPath := sstablePath + ".tmp"

	sst := &SSTable{path: tmpPath}
	if err := sst.Write(kvs); err != nil {
		return fmt.Errorf("failed to write SSTable: %w", err)
	}

	if err := fileSync(tmpPath); err != nil {
		return fmt.Errorf("failed to sync SSTable file: %w", err)
	}

	if err := os.Rename(tmpPath, sstablePath); err != nil {
		return fmt.Errorf("failed to rename SSTable file: %w", err)
	}

	sst.path = sstablePath
	if err := sst.Load(); err != nil {
		return fmt.Errorf("failed to load SSTable after writing: %w", err)
	}

	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	if err := os.Remove(walFilePath(db.dir)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove old WAL during rollover: %w", err)
	}

	newWal, err := NewWAL(db.dir)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}
	db.wal = newWal
	db.memTable = make(map[string]string)
	db.sstables = append(db.sstables, sst)

	log.Printf("Flushed %d entries to SSTable", len(kvs))

	return nil
}

func (db *DB) Close() error {
	var firstErr error
	for _, sst := range db.sstables {
		if sst != nil {
			if err := sst.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	if err := db.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func fileSync(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
