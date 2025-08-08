package db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type indexEntry struct {
	key    string
	offset int64
}

type SSTable struct {
	path  string
	index []indexEntry
}

func (s *SSTable) LinearSearch(key string) (string, bool) {
	if key == "" {
		return "", false
	}

	file, err := os.Open(s.path)
	if err != nil {
		return "", false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == key {
			return parts[1], true
		}
	}
	return "", false
}

func (s *SSTable) BinarySearch(key string) (string, bool) {
	if len(s.index) == 0 || key == "" {
		return "", false
	}

	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].key >= key
	})
	if i == len(s.index) || s.index[i].key != key {
		return "", false
	}
	off := s.index[i].offset

	file, err := os.Open(s.path)
	if err != nil {
		return "", false
	}
	defer file.Close()

	if _, err := file.Seek(off, io.SeekStart); err != nil {
		return "", false
	}

	k, err := readString(file)
	if err != nil || k != key {
		return "", false
	}

	v, err := readString(file)
	if err != nil {
		return "", false
	}
	return v, true
}

func (s *SSTable) Write(kvs [][2]string) error {
	file, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}
	defer file.Close()

	s.index = nil

	for _, kv := range kvs {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to seek in SSTable file: %w", err)
		}
		if err := writeString(file, kv[0]); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		if err := writeString(file, kv[1]); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}

		s.index = append(s.index, indexEntry{
			key:    kv[0],
			offset: offset,
		})
	}

	indexOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek to index offset: %w", err)
	}
	for _, entry := range s.index {
		if err := writeString(file, entry.key); err != nil {
			return fmt.Errorf("failed to write index key: %w", err)
		}
		if err := binary.Write(file, binary.LittleEndian, entry.offset); err != nil {
			return fmt.Errorf("failed to write index offset: %w", err)
		}
	}

	if err := binary.Write(file, binary.LittleEndian, indexOffset); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return nil
}

func (s *SSTable) Load() error {
	file, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	if stat.Size() < 8 {
		return fmt.Errorf("SSTable file is too small: %s", s.path)
	}

	_, err = file.Seek(-8, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to footer: %w", err)
	}

	var indexOffset int64
	if err := binary.Read(file, binary.LittleEndian, &indexOffset); err != nil {
		return fmt.Errorf("failed to read index offset: %w", err)
	}

	if indexOffset < 0 || indexOffset >= stat.Size()-8 {
		return fmt.Errorf("invalid index offset: %d", indexOffset)
	}

	_, err = file.Seek(indexOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to index: %w", err)
	}

	s.index = nil
	for {
		key, err := readString(file)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read index key: %w", err)
		}

		var offset int64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return fmt.Errorf("failed to read index offset: %w", err)
		}

		s.index = append(s.index, indexEntry{
			key:    key,
			offset: offset,
		})
	}

	return nil
}
