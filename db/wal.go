package db

import (
	"fmt"
	"os"
)

type WAL struct {
	file *os.File
}

func (w *WAL) Append(key, value string) error {
	if w.file == nil {
		return os.ErrInvalid
	}
	_, err := w.file.WriteString(key + "=" + value + "\n")
	if err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	return nil
}
