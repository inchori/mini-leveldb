package db

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func walFilePath(dir string) string {
	return filepath.Join(dir, "wal.log")
}

func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filePath := walFilePath(dir)
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
	return w.writeBinaryRecord(key, value)
}

func (w *WAL) Close() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer on close: %w", err)
	}

	return w.file.Close()
}

func Replay(dir string) (map[string]string, error) {
	filePath := walFilePath(dir)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil // WAL 파일 없는 건 정상
		}
		return nil, fmt.Errorf("failed to open WAL file for replay: %w", err)
	}
	defer file.Close()

	replayData := make(map[string]string)

	for {
		key, value, err := readBinaryRecord(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("invalid WAL entry, skipping: %v", err)
			continue
		}
		replayData[key] = value
	}

	return replayData, nil
}

func (w *WAL) writeBinaryRecord(key, value string) error {
	if w.writer == nil {
		return os.ErrInvalid
	}

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	data := make([]byte, 4+len(keyBytes)+4+len(valueBytes))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(keyBytes)))
	copy(data[4:4+len(keyBytes)], keyBytes)
	binary.LittleEndian.PutUint32(data[4+len(keyBytes):8+len(keyBytes)], uint32(len(valueBytes)))
	copy(data[8+len(keyBytes):], valueBytes)

	crc := crc32.ChecksumIEEE(data)

	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("failed to write record length: %w", err)
	}
	if err := binary.Write(w.writer, binary.LittleEndian, crc); err != nil {
		return fmt.Errorf("failed to write CRC: %w", err)
	}
	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL writer: %w", err)
	}

	return nil
}

func readBinaryRecord(file *os.File) (string, string, error) {
	var length, crc uint32

	if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
		return "", "", err
	}
	if err := binary.Read(file, binary.LittleEndian, &crc); err != nil {
		return "", "", err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(file, data); err != nil {
		return "", "", err
	}

	if crc32.ChecksumIEEE(data) != crc {
		return "", "", fmt.Errorf("CRC mismatch")
	}

	keyLen := binary.LittleEndian.Uint32(data[0:4])
	key := string(data[4 : 4+keyLen])
	valueLen := binary.LittleEndian.Uint32(data[4+keyLen : 8+keyLen])
	value := string(data[8+keyLen : 8+keyLen+valueLen])

	return key, value, nil
}
