// Package binser provides high-performance binary serialization for the columnar storage engine.
// It's designed to be extremely fast with zero external dependencies.
package binser

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"
)

// Supported types for binary serialization
const (
	TypeNull      byte = 0
	TypeBool      byte = 1
	TypeInt8      byte = 2
	TypeInt16     byte = 3
	TypeInt32     byte = 4
	TypeInt64     byte = 5
	TypeUint8     byte = 6
	TypeUint16    byte = 7
	TypeUint32    byte = 8
	TypeUint64    byte = 9
	TypeFloat32   byte = 10
	TypeFloat64   byte = 11
	TypeString    byte = 12
	TypeBytes     byte = 13
	TypeTime      byte = 14
	TypeArray     byte = 15
	TypeMap       byte = 16
	TypeStruct    byte = 17
	TypeTableMeta byte = 20
	TypeColMeta   byte = 21
	TypeIdxMeta   byte = 22
)

var (
	// ErrInvalidType is returned when an unsupported type is encountered
	ErrInvalidType = errors.New("binser: invalid type")
	// ErrBufferTooSmall is returned when the buffer is too small to deserialize
	ErrBufferTooSmall = errors.New("binser: buffer too small")
)

// bufferPool provides a pool of pre-allocated buffers to reduce GC pressure
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate 4KB buffers
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// getBuffer gets a buffer from the pool
func getBuffer() *[]byte {
	return bufferPool.Get().(*[]byte)
}

// putBuffer returns a buffer to the pool
func putBuffer(buf *[]byte) {
	// Clear the buffer but keep capacity
	*buf = (*buf)[:0]
	bufferPool.Put(buf)
}

// Writer provides methods to write binary data
type Writer struct {
	buf  []byte
	pool bool // Whether buf is from the pool
}

// NewWriter creates a new binary writer
func NewWriter() *Writer {
	buf := getBuffer()
	return &Writer{
		buf:  *buf,
		pool: true,
	}
}

// NewWriterWithBuffer creates a new binary writer with the provided buffer
func NewWriterWithBuffer(buf []byte) *Writer {
	return &Writer{
		buf:  buf,
		pool: false,
	}
}

// Reset resets the writer
func (w *Writer) Reset() {
	w.buf = w.buf[:0]
}

// Bytes returns the serialized bytes
func (w *Writer) Bytes() []byte {
	return w.buf
}

// Release releases the writer back to the pool
func (w *Writer) Release() {
	if w.pool {
		buf := w.buf
		w.buf = nil
		putBuffer(&buf)
	}
}

// grow ensures the buffer has enough capacity
func (w *Writer) grow(n int) {
	if cap(w.buf)-len(w.buf) < n {
		// Double the capacity
		newCap := 2*cap(w.buf) + n
		newBuf := make([]byte, len(w.buf), newCap)
		copy(newBuf, w.buf)
		w.buf = newBuf
	}
}

// WriteBool writes a boolean value
func (w *Writer) WriteBool(v bool) {
	w.grow(2)
	w.buf = append(w.buf, TypeBool)
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

// WriteInt8 writes an int8 value
func (w *Writer) WriteInt8(v int8) {
	w.grow(2)
	w.buf = append(w.buf, TypeInt8, byte(v))
}

// WriteInt16 writes an int16 value
func (w *Writer) WriteInt16(v int16) {
	w.grow(3)
	w.buf = append(w.buf, TypeInt16)
	w.buf = append(w.buf, byte(v), byte(v>>8))
}

// WriteInt32 writes an int32 value
func (w *Writer) WriteInt32(v int32) {
	w.grow(5)
	w.buf = append(w.buf, TypeInt32)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(v))
}

// WriteInt64 writes an int64 value
func (w *Writer) WriteInt64(v int64) {
	w.grow(9)
	w.buf = append(w.buf, TypeInt64)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, uint64(v))
}

// WriteUint8 writes a uint8 value
func (w *Writer) WriteUint8(v uint8) {
	w.grow(2)
	w.buf = append(w.buf, TypeUint8, v)
}

// WriteUint16 writes a uint16 value
func (w *Writer) WriteUint16(v uint16) {
	w.grow(3)
	w.buf = append(w.buf, TypeUint16)
	w.buf = append(w.buf, byte(v), byte(v>>8))
}

// WriteUint32 writes a uint32 value
func (w *Writer) WriteUint32(v uint32) {
	w.grow(5)
	w.buf = append(w.buf, TypeUint32)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, v)
}

// WriteUint64 writes a uint64 value
func (w *Writer) WriteUint64(v uint64) {
	w.grow(9)
	w.buf = append(w.buf, TypeUint64)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, v)
}

// WriteFloat32 writes a float32 value
func (w *Writer) WriteFloat32(v float32) {
	w.grow(5)
	w.buf = append(w.buf, TypeFloat32)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, math.Float32bits(v))
}

// WriteFloat64 writes a float64 value
func (w *Writer) WriteFloat64(v float64) {
	w.grow(9)
	w.buf = append(w.buf, TypeFloat64)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(v))
}

// WriteString writes a string value
func (w *Writer) WriteString(v string) {
	length := len(v)
	w.grow(1 + 4 + length)
	w.buf = append(w.buf, TypeString)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(length))
	w.buf = append(w.buf, v...)
}

// WriteBytes writes a byte slice
func (w *Writer) WriteBytes(v []byte) {
	length := len(v)
	w.grow(1 + 4 + length)
	w.buf = append(w.buf, TypeBytes)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(length))
	w.buf = append(w.buf, v...)
}

// WriteTime writes a time.Time value
func (w *Writer) WriteTime(v time.Time) {
	w.grow(9)
	w.buf = append(w.buf, TypeTime)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, uint64(v.UnixNano()))
}

// WriteArrayHeader writes an array header with length
func (w *Writer) WriteArrayHeader(length int) {
	w.grow(5)
	w.buf = append(w.buf, TypeArray)
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(length))
}

// WriteNull writes a null value
func (w *Writer) WriteNull() {
	w.grow(1)
	w.buf = append(w.buf, TypeNull)
}

// WriteByte writes a single byte directly to the buffer without a type marker
// This is useful for writing raw binary data that doesn't need type information
func (w *Writer) WriteByte(b byte) error {
	w.grow(1)
	w.buf = append(w.buf, b)
	return nil
}

// WriteInt writes an int value, optimizing for size
func (w *Writer) WriteInt(v int) {
	// Choose the smallest possible int type
	switch {
	case v >= math.MinInt8 && v <= math.MaxInt8:
		w.WriteInt8(int8(v))
	case v >= math.MinInt16 && v <= math.MaxInt16:
		w.WriteInt16(int16(v))
	case v >= math.MinInt32 && v <= math.MaxInt32:
		w.WriteInt32(int32(v))
	default:
		w.WriteInt64(int64(v))
	}
}

// WriteUint writes a uint value, optimizing for size
func (w *Writer) WriteUint(v uint) {
	// Choose the smallest possible uint type
	switch {
	case v <= math.MaxUint8:
		w.WriteUint8(uint8(v))
	case v <= math.MaxUint16:
		w.WriteUint16(uint16(v))
	case v <= math.MaxUint32:
		w.WriteUint32(uint32(v))
	default:
		w.WriteUint64(uint64(v))
	}
}

// Reader provides methods to read binary data
type Reader struct {
	buf []byte
	pos int
}

// UnreadByte steps back one byte in the buffer
func (r *Reader) UnreadByte() error {
	if r.pos > 0 {
		r.pos--
	}

	return nil
}

// NewReader creates a new binary reader
func NewReader(buf []byte) *Reader {
	return &Reader{
		buf: buf,
		pos: 0,
	}
}

// Reset resets the reader
func (r *Reader) Reset(buf []byte) {
	r.buf = buf
	r.pos = 0
}

// ReadType reads the type of the next value
func (r *Reader) ReadType() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++
	return tp, nil
}

// ReadByte reads a single byte directly from the buffer without type checking
// This is useful for reading raw binary data that doesn't have type information
func (r *Reader) ReadByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// ReadBool reads a boolean value
func (r *Reader) ReadBool() (bool, error) {
	tp, err := r.ReadType()
	if err != nil {
		return false, err
	}
	if tp != TypeBool {
		return false, ErrInvalidType
	}
	if r.pos >= len(r.buf) {
		return false, ErrBufferTooSmall
	}
	v := r.buf[r.pos] != 0
	r.pos++
	return v, nil
}

// ReadInt8 reads an int8 value
func (r *Reader) ReadInt8() (int8, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}

	if tp != TypeInt8 {
		return 0, ErrInvalidType
	}

	if r.pos >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}

	v := int8(r.buf[r.pos])
	r.pos++
	return v, nil
}

// ReadInt16 reads an int16 value
func (r *Reader) ReadInt16() (int16, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeInt16 {
		return 0, ErrInvalidType
	}
	if r.pos+1 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := int16(r.buf[r.pos]) | int16(r.buf[r.pos+1])<<8
	r.pos += 2
	return v, nil
}

// ReadInt32 reads an int32 value
func (r *Reader) ReadInt32() (int32, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeInt32 {
		return 0, ErrInvalidType
	}
	if r.pos+3 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := int32(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v, nil
}

// ReadInt64 reads an int64 value
func (r *Reader) ReadInt64() (int64, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeInt64 {
		return 0, ErrInvalidType
	}
	if r.pos+7 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := int64(binary.LittleEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

// ReadUint8 reads a uint8 value
func (r *Reader) ReadUint8() (uint8, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeUint8 {
		return 0, ErrInvalidType
	}
	if r.pos >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := r.buf[r.pos]
	r.pos++
	return v, nil
}

// ReadUint16 reads a uint16 value
func (r *Reader) ReadUint16() (uint16, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeUint16 {
		return 0, ErrInvalidType
	}
	if r.pos+1 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := uint16(r.buf[r.pos]) | uint16(r.buf[r.pos+1])<<8
	r.pos += 2
	return v, nil
}

// ReadUint32 reads a uint32 value
func (r *Reader) ReadUint32() (uint32, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeUint32 {
		return 0, ErrInvalidType
	}
	if r.pos+3 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// ReadUint64 reads a uint64 value
func (r *Reader) ReadUint64() (uint64, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeUint64 {
		return 0, ErrInvalidType
	}
	if r.pos+7 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

// ReadFloat32 reads a float32 value
func (r *Reader) ReadFloat32() (float32, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeFloat32 {
		return 0, ErrInvalidType
	}
	if r.pos+3 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := math.Float32frombits(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v, nil
}

// ReadFloat64 reads a float64 value
func (r *Reader) ReadFloat64() (float64, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}
	if tp != TypeFloat64 {
		return 0, ErrInvalidType
	}
	if r.pos+7 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

// ReadString reads a string value
func (r *Reader) ReadString() (string, error) {
	tp, err := r.ReadType()
	if err != nil {
		return "", err
	}
	if tp != TypeString {
		return "", ErrInvalidType
	}
	if r.pos+3 >= len(r.buf) {
		return "", ErrBufferTooSmall
	}
	length := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	if r.pos+length-1 >= len(r.buf) {
		return "", ErrBufferTooSmall
	}

	// Safe implementation that allocates a new string
	v := string(r.buf[r.pos : r.pos+length])

	r.pos += length
	return v, nil
}

// ReadBytes reads a byte slice
func (r *Reader) ReadBytes() ([]byte, error) {
	tp, err := r.ReadType()
	if err != nil {
		return nil, err
	}
	if tp != TypeBytes {
		return nil, ErrInvalidType
	}
	if r.pos+3 >= len(r.buf) {
		return nil, ErrBufferTooSmall
	}
	length := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4

	if r.pos+length-1 >= len(r.buf) {
		return nil, ErrBufferTooSmall
	}
	// Always create a copy to avoid buffer ownership issues
	v := make([]byte, length)
	copy(v, r.buf[r.pos:r.pos+length])
	r.pos += length
	return v, nil
}

// ReadTime reads a time.Time value
func (r *Reader) ReadTime() (time.Time, error) {
	tp, err := r.ReadType()
	if err != nil {
		return time.Time{}, err
	}
	if tp != TypeTime {
		return time.Time{}, ErrInvalidType
	}
	if r.pos+7 >= len(r.buf) {
		return time.Time{}, ErrBufferTooSmall
	}
	nanos := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return time.Unix(0, int64(nanos)), nil
}

// ReadArrayHeader reads an array header and returns the length
func (r *Reader) ReadArrayHeader() (int, error) {
	// Just read the byte directly since we know the first byte should be TypeArray
	if r.pos >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	tp := r.buf[r.pos]
	r.pos++

	if tp != TypeArray {
		return 0, ErrInvalidType
	}

	if r.pos+3 >= len(r.buf) {
		return 0, ErrBufferTooSmall
	}
	length := int(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return length, nil
}

// Marshaler is the interface implemented by types that can marshal themselves into binary
type Marshaler interface {
	MarshalBinary(w *Writer)
}

// Unmarshaler is the interface implemented by types that can unmarshal themselves from binary
type Unmarshaler interface {
	UnmarshalBinary(r *Reader) error
}

// Marshal marshals a value into binary
func Marshal(v interface{}) ([]byte, error) {
	w := NewWriter()
	defer w.Release()

	if m, ok := v.(Marshaler); ok {
		m.MarshalBinary(w)
		// Create a copy of the buffer to avoid ownership issues
		result := make([]byte, len(w.Bytes()))
		copy(result, w.Bytes())
		return result, nil
	}

	err := ErrInvalidType
	return nil, err
}

// ReadInt reads an int value, handling any numeric type
func (r *Reader) ReadInt() (int, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}

	switch tp {
	case TypeInt8:
		// Handle Int8 directly to avoid double type reading (don't call ReadInt8)
		if r.pos >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := int8(r.buf[r.pos])
		r.pos++
		return int(v), nil

	case TypeInt16:
		// Handle Int16 directly to avoid double type reading
		if r.pos+1 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := int16(r.buf[r.pos]) | int16(r.buf[r.pos+1])<<8
		r.pos += 2
		return int(v), nil

	case TypeInt32:
		// Handle Int32 directly to avoid double type reading
		if r.pos+3 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := int32(binary.LittleEndian.Uint32(r.buf[r.pos:]))
		r.pos += 4
		return int(v), nil

	case TypeInt64:
		// Handle Int64 directly to avoid double type reading
		if r.pos+7 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := int64(binary.LittleEndian.Uint64(r.buf[r.pos:]))
		r.pos += 8
		return int(v), nil

	default:
		return 0, ErrInvalidType
	}
}

// ReadUint reads a uint value, handling any numeric type
func (r *Reader) ReadUint() (uint, error) {
	tp, err := r.ReadType()
	if err != nil {
		return 0, err
	}

	switch tp {
	case TypeUint8:
		// Handle Uint8 directly to avoid double type reading
		if r.pos >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := r.buf[r.pos]
		r.pos++
		return uint(v), nil

	case TypeUint16:
		// Handle Uint16 directly to avoid double type reading
		if r.pos+1 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := uint16(r.buf[r.pos]) | uint16(r.buf[r.pos+1])<<8
		r.pos += 2
		return uint(v), nil

	case TypeUint32:
		// Handle Uint32 directly to avoid double type reading
		if r.pos+3 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := binary.LittleEndian.Uint32(r.buf[r.pos:])
		r.pos += 4
		return uint(v), nil

	case TypeUint64:
		// Handle Uint64 directly to avoid double type reading
		if r.pos+7 >= len(r.buf) {
			return 0, ErrBufferTooSmall
		}
		v := binary.LittleEndian.Uint64(r.buf[r.pos:])
		r.pos += 8
		return uint(v), nil

	default:
		return 0, ErrInvalidType
	}
}

// Unmarshal unmarshals a binary value
func Unmarshal(data []byte, v interface{}) error {
	r := NewReader(data)

	if u, ok := v.(Unmarshaler); ok {
		return u.UnmarshalBinary(r)
	}

	return ErrInvalidType
}
