package db

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/edsrzf/mmap-go"
)

type indexEntry struct {
	key    string
	offset int64
}

type SSTable struct {
	path   string
	index  []indexEntry
	filter *BloomFilter
	file   *os.File
	mmap   mmap.MMap
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
	if s.file == nil {
		return "", false
	}

	if s.filter != nil && !s.filter.MayContain(key) {
		return "", false
	}

	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].key >= key
	})
	if i == len(s.index) || s.index[i].key != key {
		return "", false
	}
	off := s.index[i].offset

	k, v, ok := readKVAt(s.file, off)
	if !ok || k != key {
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

	s.filter = NewBloomFilter(uint(len(kvs)), 0.01)

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

		s.filter.Add(kv[0])

		s.index = append(s.index, indexEntry{
			key:    kv[0],
			offset: offset,
		})
	}

	filterOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to seek to filter offset: %w", err)
	}
	if err := writeBytes(file, s.filter.bitset); err != nil {
		return fmt.Errorf("failed to write bloom filter: %w", err)
	}

	var m64, k64 uint64 = uint64(s.filter.m), uint64(s.filter.k)
	if err := binary.Write(file, binary.LittleEndian, m64); err != nil {
		return fmt.Errorf("failed to write bloom filter size: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, k64); err != nil {
		return fmt.Errorf("failed to write bloom filter hash count: %w", err)
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

	// Write the footer with index and filter offsets
	if err := binary.Write(file, binary.LittleEndian, indexOffset); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	if err := binary.Write(file, binary.LittleEndian, filterOffset); err != nil {
		return fmt.Errorf("failed to write filter offset: %w", err)
	}

	return nil
}

func (s *SSTable) Load() error {
	file, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("failed to open SSTable: %w", err)
	}
	s.file = file

	mmapData, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to mmap SSTable: %w", err)
	}
	s.mmap = mmapData

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}
	if stat.Size() < 16 {
		return fmt.Errorf("SSTable file is too small: %s", s.path)
	}

	footerStart := len(s.mmap) - 16
	indexOffset := int64(binary.LittleEndian.Uint64(s.mmap[footerStart : footerStart+8]))
	filterOffset := int64(binary.LittleEndian.Uint64(s.mmap[footerStart+8 : footerStart+16]))

	fileSize := stat.Size()
	footerPos := fileSize - 16
	if indexOffset < 0 || filterOffset < 0 {
		return fmt.Errorf("invalid negative offset in SSTable: %s", s.path)
	}
	if indexOffset > footerPos || filterOffset > footerPos {
		return fmt.Errorf("offset points beyond footer region in SSTable: %s", s.path)
	}
	if filterOffset >= indexOffset {
		return fmt.Errorf("filterOffset must be < indexOffset in SSTable: %s", s.path)
	}

	bits, offset, err := readBytesFromMmap(s.mmap, int(filterOffset))
	if err != nil {
		return fmt.Errorf("failed to read bloom bits: %w", err)
	}

	if offset+16 > len(s.mmap) {
		return fmt.Errorf("insufficient data for bloom filter metadata")
	}

	m64 := binary.LittleEndian.Uint64(s.mmap[offset : offset+8])
	k64 := binary.LittleEndian.Uint64(s.mmap[offset+8 : offset+16])
	filter := &BloomFilter{bitset: bits, m: uint(m64), k: uint(k64)}

	var index []indexEntry
	currentOffset := int(indexOffset)

	for currentOffset < len(s.mmap)-16 {
		key, newOffset, err := readStringFromMmap(s.mmap, currentOffset)
		if err != nil {
			break
		}

		if newOffset+8 > len(s.mmap)-16 {
			break
		}

		entryOffset := int64(binary.LittleEndian.Uint64(s.mmap[newOffset : newOffset+8]))
		currentOffset = newOffset + 8

		index = append(index, indexEntry{
			key:    key,
			offset: entryOffset,
		})
	}

	s.file = file
	s.filter = filter
	s.index = index

	return nil
}

func (s *SSTable) Close() error {
	var firstErr error

	if s.mmap != nil {
		if err := s.mmap.Unmap(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.mmap = nil
	}

	if s.file != nil {
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.file = nil
	}

	return firstErr
}

func readKVAt(f *os.File, off int64) (key, val string, ok bool) {
	k, next, ok := readStringAt(f, off)
	if !ok {
		return "", "", false
	}
	v, _, ok := readStringAt(f, next)
	if !ok {
		return "", "", false
	}
	return k, v, true
}

func readStringAt(f *os.File, off int64) (string, int64, bool) {
	lenBuf := make([]byte, 4)
	if _, err := f.ReadAt(lenBuf, off); err != nil {
		return "", 0, false
	}
	length := int(binary.LittleEndian.Uint32(lenBuf))

	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, off+4); err != nil {
		return "", 0, false
	}
	return string(buf), off + 4 + int64(length), true
}

func readBytesFromMmap(data []byte, offset int) ([]byte, int, error) {
	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for length prefix")
	}

	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	newOffset := offset + 4

	if newOffset+length > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for bytes payload")
	}

	result := make([]byte, length)
	copy(result, data[newOffset:newOffset+length])

	return result, newOffset + length, nil
}

func readStringFromMmap(data []byte, offset int) (string, int, error) {
	if offset+4 > len(data) {
		return "", 0, fmt.Errorf("insufficient data for length prefix")
	}

	length := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	newOffset := offset + 4

	if newOffset+length > len(data) {
		return "", 0, fmt.Errorf("insufficient data for string payload")
	}

	result := string(data[newOffset : newOffset+length])

	return result, newOffset + length, nil
}
