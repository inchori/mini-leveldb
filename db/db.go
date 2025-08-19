package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type DB struct {
	memTable        map[string]string
	wal             *WAL
	sstables        []*SSTable
	dir             string
	compactionLimit int
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
		memTable:        memTable,
		wal:             wal,
		dir:             dir,
		compactionLimit: 5,
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

type GetResult struct {
	Value string
	Error error
}

func (db *DB) GetBatch(keys []string) []GetResult {
	results := make([]GetResult, len(keys))

	for i, key := range keys {
		value, err := db.Get(key)
		results[i] = GetResult{
			Value: value,
			Error: err,
		}
	}

	return results
}

func (db *DB) GetBatchParallel(keys []string) []GetResult {
	results := make([]GetResult, len(keys))
	var wg sync.WaitGroup

	for i, key := range keys {
		wg.Add(1)
		go func(index int, k string) {
			defer wg.Done()
			value, err := db.Get(k)
			results[index] = GetResult{
				Value: value,
				Error: err,
			}
		}(i, key)
	}

	wg.Wait()
	return results
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

func (db *DB) PutBatch(kvs [][2]string) error {
	if len(kvs) == 0 {
		return nil
	}

	for _, kv := range kvs {
		if kv[0] == "" {
			return fmt.Errorf("failed to put batch: key cannot be empty")
		}
	}

	if err := db.wal.AppendBatch(kvs); err != nil {
		return fmt.Errorf("failed to append batch to WAL: %w", err)
	}

	for _, kv := range kvs {
		db.memTable[kv[0]] = kv[1]
	}

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

	if len(db.sstables) >= db.compactionLimit {
		if err := db.compact(); err != nil {
			log.Printf("Compaction failed: %v", err)
		}
	}

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

func (db *DB) compact() error {
	if len(db.sstables) < 2 {
		return nil
	}

	log.Printf("Starting compaction with %d SSTable files", len(db.sstables))

	mergeCount := len(db.sstables) / 2
	if mergeCount < 2 {
		mergeCount = 2
	}

	toMerge := db.sstables[:mergeCount]
	remaining := db.sstables[mergeCount:]

	allKVs := make(map[string]string)

	for _, sst := range toMerge {
		kvs, err := db.extractAllKVsFromSSTable(sst)
		if err != nil {
			return fmt.Errorf("failed to extract KVs from SSTable: %w", err)
		}
		for _, kv := range kvs {
			allKVs[kv[0]] = kv[1]
		}
	}

	sortedKVs := make([][2]string, 0, len(allKVs))
	keys := make([]string, 0, len(allKVs))
	for k := range allKVs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sortedKVs = append(sortedKVs, [2]string{k, allKVs[k]})
	}

	filename := fmt.Sprintf("sstable_compacted_%d.sst", time.Now().UnixNano())
	sstablePath := filepath.Join(db.dir, filename)
	tmpPath := sstablePath + ".tmp"

	newSST := &SSTable{path: tmpPath}
	if err := newSST.Write(sortedKVs); err != nil {
		return fmt.Errorf("failed to write compacted SSTable: %w", err)
	}

	if err := fileSync(tmpPath); err != nil {
		return fmt.Errorf("failed to sync compacted SSTable: %w", err)
	}

	if err := os.Rename(tmpPath, sstablePath); err != nil {
		return fmt.Errorf("failed to rename compacted SSTable: %w", err)
	}

	newSST.path = sstablePath
	if err := newSST.Load(); err != nil {
		return fmt.Errorf("failed to load compacted SSTable: %w", err)
	}

	for _, sst := range toMerge {
		if err := sst.Close(); err != nil {
			log.Printf("Warning: failed to close SSTable during compaction: %v", err)
		}
		if err := os.Remove(sst.path); err != nil {
			log.Printf("Warning: failed to remove old SSTable file: %v", err)
		}
	}

	db.sstables = append([]*SSTable{newSST}, remaining...)

	log.Printf("Compaction completed: merged %d files into 1, %d files remaining",
		mergeCount, len(db.sstables))

	return nil
}

func (db *DB) extractAllKVsFromSSTable(sst *SSTable) ([][2]string, error) {
	var kvs [][2]string

	for _, entry := range sst.index {
		key, value, ok := sst.readKVFromMmap(entry.offset)
		if !ok {
			continue
		}
		kvs = append(kvs, [2]string{key, value})
	}

	return kvs, nil
}

func fileSync(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
