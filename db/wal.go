package db

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func NewWAL(dir string) (*WAL, error) {
	walDataPath := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDataPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filePath := filepath.Join(walDataPath, "wal.log")
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	writer := bufio.NewWriter(file)

	return &WAL{
		file:   file,
		writer: writer,
	}, nil
}

func (w *WAL) Append(key, value string) error {
	if w.writer == nil {
		return os.ErrInvalid
	}

	_, err := w.writer.WriteString(key + "=" + value + "\n")
	if err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer: %w", err)
	}

	return nil
}

func (w *WAL) Close() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer on close: %w", err)
	}

	return w.file.Close()
}

func Replay(dir string) (map[string]string, error) {
	filePath := filepath.Join(dir, "wal.log")

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil // WAL 파일 없는 건 정상
		}
		return nil, fmt.Errorf("failed to open WAL file for replay: %w", err)
	}
	defer file.Close()

	replayData := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			log.Printf("invalid WAL entry: %q", line)
			continue
		}

		key := parts[0]
		value := parts[1]

		replayData[key] = value
	}

	return replayData, nil
}
