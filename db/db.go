package db

import "fmt"

type DB struct {
	memTable map[string]string
}

func NewDB() *DB {
	return &DB{
		memTable: make(map[string]string),
	}
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
	db.memTable[key] = value
	return nil
}
