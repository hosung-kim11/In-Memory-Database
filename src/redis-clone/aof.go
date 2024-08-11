package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

/*
desc: initialize a Apend-Only File
input: path, file location
output:
	- *Aof, pointer to aof
	- error, errors
*/
func NewAof(path string) (*Aof, error) {
	// create file if not exist with right perms
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd: bufio.NewReader(f),
	}

	// keep file updated and saved
	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

/*
desc: close aof safely
input: aof, pointer to Aof
*/
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

/*
desc: write value to aof file
reciever, aof, aof file
input: value, content to be written
*/
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

/*
desc: reads all Value obj in aof and executes to restore database
*/
func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	// start from beginning
	aof.file.Seek(0, io.SeekStart)

	reader := NewResp(aof.file)

	// read till end
	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}
		fn(value)
	}

	return nil
}