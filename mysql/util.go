// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package mysql

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"time"
	"unicode/utf8"
)

func Pstack() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[0:n])
}

func CalcPassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// seed must be in the range of ascii
func RandomBuf(size int) ([]byte, error) {
	buf := make([]byte, size)
	rand.Seed(time.Now().UTC().UnixNano())
	min, max := 30, 127
	for i := 0; i < size; i++ {
		buf[i] = byte(min + rand.Intn(max-min))
	}
	return buf, nil
}

//
// Encoding methods.
//
// The same assumptions are made for all the encoding functions:
// - there is enough space to write the data in the buffer. If not, we
// will panic with out of bounds.
// - all functions start writing at 'pos' in the buffer, and return the next position.

// lenEncIntSize returns the number of bytes required to encode a
// variable-length integer.
func LenEncIntSize(i uint64) int {
	switch {
	case i < 251:
		return 1
	case i < 1<<16:
		return 3
	case i < 1<<24:
		return 4
	default:
		return 9
	}
}

func writeLenEncInt(data []byte, pos int, i uint64) int {
	switch {
	case i < 251:
		data[pos] = byte(i)
		return pos + 1
	case i < 1<<16:
		data[pos] = 0xfc
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		return pos + 3
	case i < 1<<24:
		data[pos] = 0xfd
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		data[pos+4] = byte(i >> 24)
		data[pos+5] = byte(i >> 32)
		data[pos+6] = byte(i >> 40)
		data[pos+7] = byte(i >> 48)
		data[pos+8] = byte(i >> 56)
		return pos + 9
	}
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	switch b[0] {

	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

	// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

	// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

	// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

func PutLengthEncodedInt(n uint64) []byte {
	switch {
	case n <= 250:
		return []byte{byte(n)}

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	case n <= 0xffffffffffffffff:
		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}
	return nil
}

func LengthEnodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	// Modify: Differentiate between nil and empty string for different handling,
	//         an empty string should return an empty byte array.
	num, isNull, n := LengthEncodedInt(b)
	if isNull {
		return nil, isNull, n, nil
	}
	if num < 1 {
		return []byte{}, isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

func SkipLengthEnodedString(b []byte) (int, error) {
	// Get length
	num, _, n := LengthEncodedInt(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

func PutLengthEncodedString(b []byte) []byte {
	data := make([]byte, 0, len(b)+9)
	data = append(data, PutLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

func Uint16ToBytes(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func Uint32ToBytes(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func Uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func FormatBinaryDate(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	default:
		return nil, fmt.Errorf("invalid date packet length %d", n)
	}
}

func FormatBinaryDateTime(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00 00:00:00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	case 7:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6])), nil
	case 11:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d.%06d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6],
			binary.LittleEndian.Uint32(data[7:11]))), nil
	default:
		return nil, fmt.Errorf("invalid datetime packet length %d", n)
	}
}

func FormatBinaryTime(n int, data []byte) ([]byte, error) {
	if n == 0 {
		return []byte("0000-00-00"), nil
	}

	var sign byte
	if data[0] == 1 {
		sign = byte('-')
	}

	switch n {
	case 8:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
		)), nil
	case 12:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d.%06d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
			binary.LittleEndian.Uint32(data[8:12]),
		)), nil
	default:
		return nil, fmt.Errorf("invalid time packet length %d", n)
	}
}

var (
	DONTESCAPE = byte(255)

	EncodeMap [256]byte
)

func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))

	for i, w := 0, 0; i < len(sql); i += w {
		runeValue, width := utf8.DecodeRuneInString(sql[i:])
		if c := EncodeMap[byte(runeValue)]; c == DONTESCAPE {
			dest = append(dest, sql[i:i+width]...)
		} else {
			dest = append(dest, '\\', c)
		}
		w = width
	}

	return string(dest)
}

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range EncodeMap {
		EncodeMap[i] = DONTESCAPE
	}
	for i := range EncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			EncodeMap[byte(i)] = to
		}
	}
}
