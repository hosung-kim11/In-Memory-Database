package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING = '+' // simple string
	ERROR = '-'
	INTEGER = ':'
	BULK = '$' // bulk strings
	ARRAY = '*'
)

type Value struct {
	typ string
	str string
	num int
	bulk string
	array []Value
}

// reader points to a bufio reader object
type Resp struct {
	reader *bufio.Reader
}

// create an instance of new Resp struct with a reader
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

/* 
desc: reads bytes until the speacial stop char of '\r'
receiver: r, a pointer to a Resp
output: 
	- line, the line that was read
	- n, number of bytes read
	- err, any errors
*/
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

/* 
desc: reads a line and parsis line into an int
receiver: r, a pointer to a Resp
output: 
	- x, the int parsed from line (base 10, 64 bit size)
	- n, number of bytes read
	- err, errors
*/
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

/*
desc: determines the right type to parse
receiver: r, a pointer to a Resp
output: 
	- Value, the parsed message
	- err, errors
*/
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

/* 
desc: returns elements in an arary
receiver: r, a pointer to a resp
output:
	- Value, elements in an array
	- error, errors
*/
func (r *Resp) readArray() (Value, error) {
	// create new Value struct
	v := Value{}
	v.typ = "array"

	// get len of array from string input
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// read each element
	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array = append(v.array, val)
	}
	return v, nil
}

/* 
desc: reads a bulk string
receiver: r, a pointer to a resp
output:
	- Value, bulk string content
	- error, errors 
*/
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	
	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.bulk = string(bulk)
	r.readLine()

	return v, nil
}

/* 
desc: determine the right write function
receiver: v, Value struct
*/
func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

/*
desc: create a RESP Simple String
receiver: v, Value struct
output: a serialized byte slice
			+lebron\r\n
*/
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

/*
desc: create a RESP Bulk String
receiver: v, Value struct
output: a serialized byte slice
			$6\r\nlebron\r\n
*/
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes  = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

/*
desc: create a RESP Array
receiver: v, Value struct
output: a serialized byte slice
		 *3$\r\n<elem>\r\n<elem>\r\n<elem>
*/
func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

/*
desc: create a RESP Simple Error
receiver: v, Value struct
output: a serialized byte slice 
			-Error message\r\n
*/
func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

/*
desc: create a RESP null value
receiver: v, Value struct
output: byte slice with null val
*/
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer : w}
}

/*
desc: convert a value in bytes and sends to a writer
receiver: w, pointer to Writer
input: v, Value
*/
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}