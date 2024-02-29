package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING      = '+'
	BULK_STRING = '$'
	INTEGER     = ':'
	ARRAY       = '*'

	TypeBulkString = "bulk_string"
	TypeString     = "string"
	TypeArray      = "array"
)

type Value struct {
	Type       string
	String     string
	Number     int
	BulkString string
	Array      []Value
}

type RESPReader struct {
	reader *bufio.Reader
}
type RESPWriter struct {
	writer *bufio.Writer
}

func NewRESPWriter(writer io.Writer) *RESPWriter {
	return &RESPWriter{
		writer: bufio.NewWriter(writer),
	}
}
func NewRESPReader(reader io.Reader) *RESPReader {
	return &RESPReader{reader: bufio.NewReader(reader)}
}
func (r *RESPReader) readLine() ([]byte, int, error) {
	line := make([]byte, 0)
	n := 0
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) > 1 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}
func (r *RESPReader) readInteger() (int, int, error) {
	value, n, err := r.readLine()
	fmt.Printf("integer value: %s\n", value) //
	if err != nil {
		return 0, 0, nil
	}
	i64, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *RESPReader) readBulkString() (Value, error) {
	v := Value{}
	v.Type = TypeBulkString
	len, _, err := r.readInteger()
	v.Number = len
	if err != nil {
		return Value{}, err
	}
	bulkString := make([]byte, int(len))
	_, err = r.reader.Read(bulkString)
	if err != nil {
		return v, err
	}
	v.BulkString = string(bulkString)
	err = r.passCRLF()
	if err != nil {
		return v, err
	}
	return v, nil
}
func (r *RESPReader) readArray() (Value, error) {
	v := Value{}
	v.Type = TypeArray
	len, _, err := r.readInteger()
	v.Number = len
	if err != nil {
		return v, nil
	}
	v.Array = make([]Value, 0)

	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array = append(v.Array, val)
	}
	return v, nil
}
func (r *RESPReader) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch _type {
	case BULK_STRING:
		return r.readBulkString()
	case ARRAY:
		return r.readArray()
	default:
		return Value{}, nil
	}
}

func (r *RESPReader) passCRLF() error {
	_, _, err := r.readLine()
	return err
}
func (w *RESPWriter) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}
func (v Value) Marshal() []byte {
	switch v.Type {
	case TypeString:
		return v.marshalString()

	case TypeArray:
		return v.marshalArray()

	case TypeBulkString:
		return v.marshalBulkString()

	default:
		return []byte{}
	}
}
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, []byte(v.String)...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}
func (v Value) marshalBulkString() []byte {
	var bytes []byte
	bytes = append(bytes, BULK_STRING)
	bytes = append(bytes, []byte(strconv.Itoa(v.Number))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.BulkString...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}
func (v Value) marshalArray() []byte {
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, []byte(strconv.Itoa(len(v.Array)))...)
	bytes = append(bytes, '\r', '\n')
	for _, val := range v.Array {
		bytes = append(bytes, val.Marshal()...)
	}
	bytes = append(bytes, '\r', '\n')
	return bytes
}
