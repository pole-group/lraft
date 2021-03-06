// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"bytes"
	"fmt"
	"hash/crc64"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
)

const (
	Int64MaxValue = int64(0x7fffffffffffffff)
)

type AtomicInt64 struct {
	v int64
}

func NewAtomicInt64() AtomicInt64 {
	return AtomicInt64{v: 0}
}

func (a AtomicInt64) Increment() int64 {
	return atomic.AddInt64(&a.v, 1)
}

func (a AtomicInt64) Value() int64 {
	return atomic.LoadInt64(&a.v)
}

func (a AtomicInt64) Decrement() int64 {
	return atomic.AddInt64(&a.v, -1)
}

func AnalyzeIPAndPort(address string) (ip string, port int) {
	s := strings.Split(address, ":")
	port, err := strconv.Atoi(s[1])
	CheckErr(err)
	return s[0], port
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

func ParseToInt(val string) int {
	i, err := strconv.Atoi(val)
	CheckErr(err)
	return i
}

func ParseToInt64(val string) int64 {
	i, err := strconv.Atoi(val)
	CheckErr(err)
	return int64(i)
}

func ParseToInt32(val string) int32 {
	i, err := strconv.Atoi(val)
	CheckErr(err)
	return int32(i)
}

func ParseToInt16(val string) int16 {
	i, err := strconv.Atoi(val)
	CheckErr(err)
	return int16(i)
}

func ParseToInt8(val string) int8 {
	i, err := strconv.Atoi(val)
	CheckErr(err)
	return int8(i)
}

func ParseToUint64(val string) uint64 {
	i, err := strconv.ParseUint(val, 10, 64)
	CheckErr(err)
	return i
}

func ParseToUint32(val string) uint32 {
	i, err := strconv.ParseUint(val, 10, 32)
	CheckErr(err)
	return uint32(i)
}

func ParseToUint16(val string) uint16 {
	i, err := strconv.ParseUint(val, 10, 16)
	CheckErr(err)
	return uint16(i)
}

func ParseToUint8(val string) uint8 {
	i, err := strconv.ParseUint(val, 10, 8)
	CheckErr(err)
	return uint8(i)
}

var (
	Crc64Table = crc64.MakeTable(uint64(528))
)

func Checksum(b []byte) uint64 {
	return crc64.Checksum(b, Crc64Table)
}

func Checksum2Long(a, b uint64) uint64 {
	return a ^ b
}

const (
	ErrNonNilMsg = "%s must not nil"
)

func IF(expression bool, a, b interface{}) interface{} {
	if expression {
		return a
	}
	return b
}

func RequireNonNil(e interface{}, msg string) (interface{}, error) {
	if e == nil {
		return nil, errors.Errorf(ErrNonNilMsg, msg)
	}
	return e, nil
}

func RequireTrue(expression bool, format string, args ...interface{}) error {
	if !expression {
		return errors.Errorf(format, args)
	}
	return nil
}

func RequireFalse(expression bool, format string, args ...interface{}) {
	if expression {
		panic(errors.Errorf(format, args))
	}
}

func StringFormat(format string, args ...interface{}) string {
	buf := bytes.NewBuffer([]byte{})
	_, err := fmt.Fprintf(buf, format, args)
	CheckErr(err)
	return string(buf.Bytes())
}

func GetInt64FormBytes(memory []byte, index int) int64 {
	return int64(memory[index]&0xff)<<56 | int64(memory[index+1]&0xff)<<48 | int64(
		memory[index+2]&0xff)<<40 | int64(memory[index+3]&0xff)<<32 | int64(memory[index+4]&0xff)<<24 | int64(
		memory[index+5]&0xff)<<16 | int64(memory[index+6]&0xff)<<8 | int64(memory[index+7]&0xff)
}
