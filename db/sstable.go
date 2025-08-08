package db

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type SSTable struct {
	path string
}

// TODO: Apply Binary Search for better performance
func (s *SSTable) Search(key string) (string, bool) {
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

func (s *SSTable) Write(kvs [][2]string) error {
	file, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create SSTable: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, kv := range kvs {
		line := fmt.Sprintf("%s\t%s\n", kv[0], kv[1])
		if _, err := writer.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to SSTable: %w", err)
		}
	}
	return writer.Flush()
}
