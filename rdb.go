package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
)

type RDBReader struct {
	file   *os.File
	reader *bufio.Reader
}

func NewRDBReader(dir, filename string) (*RDBReader, error) {
	path := fmt.Sprintf("./%s/%s", dir, filename)
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

	if string(header) != "REDIS0012" {
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
		return nil, fmt.Errorf("error reading RDB header: %v", err)
	}

	database := make(map[string]string)

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading RDB file: %v", err)
		}

		switch b {
		case 0xFE: // Database selector
			if _, err := r.readSize(); err != nil {
				return nil, fmt.Errorf("error reading database selector: %v", err)
			}
		case 0xFA: // Metadata key-value pair
			if _, err := r.readString(); err != nil {
				return nil, fmt.Errorf("error reading metadata key: %v", err)
			}
			if _, err := r.readString(); err != nil {
				return nil, fmt.Errorf("error reading metadata value: %v", err)
			}
		case 0xFB: // Hash table size
			if _, err := r.readSize(); err != nil {
				return nil, fmt.Errorf("error reading key hash table size: %v", err)
			}
			if _, err := r.readSize(); err != nil {
				return nil, fmt.Errorf("error reading expire hash table size: %v", err)
			}
		case 0x00: // Key-value pair
			key, err := r.readString()
			if err != nil {
				return nil, fmt.Errorf("error reading key: %v", err)
			}
			value, err := r.readString()
			if err != nil {
				return nil, fmt.Errorf("error reading value: %v", err)
			}
			database[key] = value
		case 0xFF: // End of file marker
			return database, nil
		default:
			return nil, fmt.Errorf("unexpected byte in RDB file: 0x%02X", b)
		}
	}

	return database, fmt.Errorf("unexpected end of RDB file")
}

func (r *RDBReader) readString() (string, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return "", err
	}

	if b>>6 != 3 { // Standard string encoding
		r.reader.UnreadByte()
		size, err := r.readSize()
		if err != nil {
			return "", err
		}

		data := make([]byte, size)
		if _, err = io.ReadFull(r.reader, data); err != nil {
			return "", err
		}
		return string(data), nil
	}

	switch b & 0x3F { // Special encoding cases
	case 0: // 8-bit integer
		intVal, err := r.reader.ReadByte()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(intVal)), nil
	case 1: // 16-bit integer
		intVal, err := r.readUint16()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(intVal)), nil
	case 2: // 32-bit integer
		intVal, err := r.readUint32()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(intVal)), nil
	default:
		return "", fmt.Errorf("unsupported special string encoding: 0x%02X", b)
	}
}

func (r *RDBReader) readUint16() (uint16, error) {
	buf := make([]byte, 2)
	_, err := io.ReadFull(r.reader, buf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf), nil
}

func (r *RDBReader) readUint32() (uint32, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r.reader, buf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

/*
00	The next 6 bits represent the length
01	Read one additional byte. The combined 14 bits represent the length
10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
11	The next object is encoded in a special format. The remaining 6 bits indicate the format.
*/
func (r *RDBReader) readSize() (uint64, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch b >> 6 {
	case 0:
		size := uint64(b & 0x3F)
		return size, nil
	case 1:
		next, err := r.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		size := (uint64(b&0x3F) << 8) | uint64(next)
		return size, nil
	case 2:
		buf := make([]byte, 4)
		_, err := io.ReadFull(r.reader, buf)
		if err != nil {
			return 0, err
		}
		size := uint64(binary.BigEndian.Uint32(buf))
		return size, nil
	case 3:
		// This is a special encoding
		// We'll handle this in readString
		return 0, fmt.Errorf("special encoding")
	default:
		return 0, fmt.Errorf("invalid size encoding")
	}
}

