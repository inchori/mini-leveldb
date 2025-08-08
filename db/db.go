package db

import (
	"fmt"
	"log"
	"path"
	"path/filepath"
	"sort"
	"time"
)

type DB struct {
	memTable map[string]string
	wal      *WAL
	ssTables []*SSTable
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

	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, fmt.Errorf("failed to list SSTable files: %w", err)
	}
	sort.Strings(files)

	var ssTables []*SSTable
	for _, file := range files {
		ssTables = append(ssTables, &SSTable{path: file})
	}

	return &DB{
		memTable: memTable,
		wal:      wal,
		ssTables: ssTables,
	}, nil
}

func (db *DB) Get(key string) (string, error) {
	if value, ok := db.memTable[key]; ok {
		return value, nil
	}

	for i := len(db.ssTables) - 1; i >= 0; i-- {
		if value, ok := db.ssTables[i].Search(key); ok {
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

func (db *DB) Flush(dir string) error {
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
	ssTablePath := path.Join(dir, filename)
	sst := &SSTable{path: ssTablePath}
	if err := sst.Write(kvs); err != nil {
		return fmt.Errorf("failed to write SSTable: %w", err)
	}

	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	newWal, err := NewWAL(dir)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}
	db.wal = newWal
	db.memTable = make(map[string]string)
	db.ssTables = append(db.ssTables, sst)
	log.Printf("Flushed %d entries to SSTable", len(kvs))

	return nil
}

func (db *DB) Close() error {
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	return nil
}
