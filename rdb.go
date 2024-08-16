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
			fmt.Printf("File does not exist: %s\n", path)
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
		fmt.Printf("Error reading header: %v\n", err)
		return nil, err
	}
	fmt.Println("Header read successfully")

	database := make(map[string]string)

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		fmt.Printf("Read byte: 0x%02X\n", b)
		switch b {
		case 0xFE: // database selector
			fmt.Println("Database selector found")
			dbNum, err := r.readSize()
			if err != nil {
				fmt.Printf("Error reading database number: %v\n", err)
				return nil, err
			}
			fmt.Printf("Database number: %d\n", dbNum)
		case 0xFA: // meta data key value
			fmt.Println("Metadata entry found")
			metaKey, err := r.readString()
			if err != nil {
				fmt.Printf("Error reading metadata key: %v\n", err)
				return nil, err
			}
			metaValue, err := r.readString()
			if err != nil {
				fmt.Printf("Error reading metadata value: %v\n", err)
				return nil, err
			}
			fmt.Printf("Metadata: %s = %s\n", metaKey, metaValue)
		case 0xFB: // hash table size
			fmt.Println("Hash table size info found")
			keySize, err := r.readSize()
			if err != nil {
				fmt.Printf("Error reading key hash table size: %v\n", err)
				return nil, err
			}
			expireSize, err := r.readSize()
			if err != nil {
				fmt.Printf("Error reading expire hash table size: %v\n", err)
				return nil, err
			}
			fmt.Printf("Hash table sizes - Keys: %d, Expires: %d\n", keySize, expireSize)
		case 0x00: // start of key value; load into db
			fmt.Println("Start of key-value pair")
			key, err := r.readString()
			if err != nil {
				fmt.Printf("Error reading key: %v\n", err)
				return nil, err
			}
			value, err := r.readString()
			if err != nil {
				fmt.Printf("Error reading value: %v\n", err)
				return nil, err
			}
			fmt.Printf("Read key-value pair: %s = %s\n", key, value)
			database[key] = value

		case 0xFF: // EOF
			fmt.Println("End of file marker found")
			return database, nil
		default:
			return nil, fmt.Errorf("unexpected byte: 0x%02X", b)
		}
	}
}
func (r *RDBReader) readString() (string, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return "", err
	}

	fmt.Printf("String encoding byte: 0x%02X\n", b)

	if b>>6 != 3 {
		// Standard string encoding
		r.reader.UnreadByte()
		size, err := r.readSize()
		if err != nil {
			return "", err
		}
		fmt.Printf("String size: %d\n", size)

		data := make([]byte, size)
		_, err = io.ReadFull(r.reader, data)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	// Special encoding
	switch b & 0x3F {
	case 0: // 8 bit integer
		intVal, err := r.reader.ReadByte()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(intVal)), nil
	case 1: // 16 bit integer
		intVal, err := r.readUint16()
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(intVal)), nil
	case 2: // 32 bit integer
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

	fmt.Printf("Size encoding byte: 0x%02X\n", b)

	switch b >> 6 {
	case 0:
		size := uint64(b & 0x3F)
		fmt.Printf("6-bit size: %d\n", size)
		return size, nil
	case 1:
		next, err := r.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		size := (uint64(b&0x3F) << 8) | uint64(next)
		fmt.Printf("14-bit size: %d\n", size)
		return size, nil
	case 2:
		buf := make([]byte, 4)
		_, err := io.ReadFull(r.reader, buf)
		if err != nil {
			return 0, err
		}
		size := uint64(binary.BigEndian.Uint32(buf))
		fmt.Printf("32-bit size: %d\n", size)
		return size, nil
	case 3:
		// This is a special encoding
		// We'll handle this in readString
		return 0, fmt.Errorf("special encoding")
	default:
		return 0, fmt.Errorf("invalid size encoding")
	}
}
