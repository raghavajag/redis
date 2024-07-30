package main

import (
	"bufio"
	"fmt"
	"os"
)

type RDBReader struct {
	file   *os.File
	reader *bufio.Reader
}

func NewRDBReader(dir, filename string) (*RDBReader, error) {
	path := fmt.Sprintf("%s/%s", dir, filename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &RDBReader{file: nil, reader: nil}, nil // File doesn't exist, return empty reader
		}
		return nil, err
	}
	return &RDBReader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (r *RDBReader) Close() {
	if r.file != nil {
		r.file.Close()
	}
}
func (r *RDBReader) readHeader() error {
	if r.file == nil {
		return nil // Empty database
	}

	header := make([]byte, 9)
	_, err := r.file.Read(header)
	if err != nil {
		return err
	}

	if string(header) != "REDIS0007" {
		return fmt.Errorf("invalid RDB file format")
	}

	return nil
}
func (r *RDBReader) ReadDatabase() (map[string]string, error) {
	if r.file == nil {
		return make(map[string]string), nil // Empty database
	}

	err := r.readHeader()
	if err != nil {
		return nil, err
	}

	database := make(map[string]string)

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		switch b {
		case 0xFE: // Database selector
		case 0xFB: // Hash table size info
		case 0xFF: // End of file
			return database, nil
		default:
			// Read key, value and expiry, and store in database map.
		}
	}
}
