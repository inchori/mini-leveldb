package db

import "fmt"

type DB struct {
	memTable map[string]string
	wal      *WAL
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

	return &DB{
		memTable: memTable,
		wal:      wal,
	}, nil
}

func (db *DB) Get(key string) (string, error) {
	if value, ok := db.memTable[key]; ok {
		return value, nil
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

func (db *DB) Close() error {
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	return nil
}
