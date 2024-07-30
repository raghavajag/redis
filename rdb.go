package main

import (
	"fmt"
	"os"
)

type RDBReader struct {
	file *os.File
}

func NewRDBReader(dir, filename string) (*RDBReader, error) {
	path := fmt.Sprintf("%s%s", dir, filename)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &RDBReader{file: nil}, nil
		}
		return nil, err
	}
	return &RDBReader{file: file}, nil
}
func (r *RDBReader) Close() {
	if r.file != nil {
		r.file.Close()
	}
}
