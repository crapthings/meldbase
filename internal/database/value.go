package database

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"time"
)

// Kind identifies the type stored in a Value.
type Kind uint8

const (
	// NullKind identifies a null value.
	NullKind Kind = iota
	// BoolKind identifies a boolean value.
	BoolKind
	// Int64Kind identifies a signed 64-bit integer.
	Int64Kind
	// Float64Kind identifies a 64-bit float.
	Float64Kind
	// StringKind identifies a string.
	StringKind
	// BinaryKind identifies binary data.
	BinaryKind
	// TimeKind identifies a timestamp.
	TimeKind
	// ArrayKind identifies an array of values.
	ArrayKind
	// ObjectKind identifies a document object.
	ObjectKind
	// IDKind identifies a document identifier.
	IDKind
)

// DocumentID is a 16-byte document identifier.
type DocumentID [16]byte

// NewDocumentID creates an identifier from cryptographically random bytes.
func NewDocumentID() (DocumentID, error) {
	var id DocumentID
	_, err := rand.Read(id[:])
	return id, err
}

// ParseDocumentID reads a 16-byte identifier from a hexadecimal string.
func ParseDocumentID(s string) (DocumentID, error) {
	var id DocumentID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, fmt.Errorf("%w: malformed document id", ErrInvalidDocument)
	}
	copy(id[:], b)
	return id, nil
}

// String returns the identifier as lowercase hexadecimal text.
func (id DocumentID) String() string { return hex.EncodeToString(id[:]) }

// IsZero reports whether every byte of the identifier is zero.
func (id DocumentID) IsZero() bool { return id == DocumentID{} }

// Value stores typed database data behind a private representation.
type Value struct {
	kind Kind
	b    bool
	i    int64
	f    float64
	s    string
	bin  []byte
	t    time.Time
	arr  []Value
	obj  Document
	id   DocumentID
}

// Null creates a null value.
func Null() Value { return Value{kind: NullKind} }

// Bool creates a boolean value.
func Bool(v bool) Value { return Value{kind: BoolKind, b: v} }

// Int creates a signed 64-bit integer value.
func Int(v int64) Value { return Value{kind: Int64Kind, i: v} }

// Float creates a 64-bit floating-point value.
func Float(v float64) Value { return Value{kind: Float64Kind, f: v} }

// String creates a string value.
func String(v string) Value { return Value{kind: StringKind, s: v} }

// Binary creates a binary value from a copy of the bytes.
func Binary(v []byte) Value { return Value{kind: BinaryKind, bin: append([]byte(nil), v...)} }

// Time creates a UTC timestamp value rounded down to millisecond precision.
func Time(v time.Time) Value {
	return Value{kind: TimeKind, t: v.Round(0).UTC().Truncate(time.Millisecond)}
}

// ID creates a document identifier value.
func ID(v DocumentID) Value { return Value{kind: IDKind, id: v} }

// Array creates an array value from copies of its elements.
func Array(v ...Value) Value { return Value{kind: ArrayKind, arr: cloneValues(v)} }

// Object creates an object value from a copy of the document.
func Object(v Document) Value { return Value{kind: ObjectKind, obj: v.Clone()} }

// Kind returns the type of the value.
func (v Value) Kind() Kind { return v.kind }

// Bool returns the stored boolean and whether the value is a boolean.
func (v Value) Bool() (bool, bool) { return v.b, v.kind == BoolKind }

// Int64 returns the stored integer and whether the value is an integer.
func (v Value) Int64() (int64, bool) { return v.i, v.kind == Int64Kind }

// Float64 returns the stored float and whether the value is a float.
func (v Value) Float64() (float64, bool) { return v.f, v.kind == Float64Kind }

// StringValue returns the stored string and whether the value is a string.
func (v Value) StringValue() (string, bool) { return v.s, v.kind == StringKind }

// TimeValue returns the stored time and whether the value is a time.
func (v Value) TimeValue() (time.Time, bool) { return v.t, v.kind == TimeKind }

// IDValue returns the stored identifier and whether the value is an identifier.
func (v Value) IDValue() (DocumentID, bool) { return v.id, v.kind == IDKind }

// BinaryValue returns a copy of the stored bytes and whether the value is binary.
func (v Value) BinaryValue() ([]byte, bool) {
	return append([]byte(nil), v.bin...), v.kind == BinaryKind
}

// ArrayValue returns copies of the stored elements and whether the value is an array.
func (v Value) ArrayValue() ([]Value, bool) { return cloneValues(v.arr), v.kind == ArrayKind }

// ObjectValue returns a copy of the stored document and whether the value is an object.
func (v Value) ObjectValue() (Document, bool) { return v.obj.Clone(), v.kind == ObjectKind }

// Clone copies the value and all nested data.
func (v Value) Clone() Value {
	v.bin = append([]byte(nil), v.bin...)
	v.arr = cloneValues(v.arr)
	v.obj = v.obj.Clone()
	return v
}

// Equal compares values recursively and allows exact equality between integers and floats.
func (v Value) Equal(other Value) bool {
	if v.kind != other.kind {
		if comparison, ok := compareNumeric(v, other); ok {
			return comparison == 0
		}
		return false
	}
	switch v.kind {
	case NullKind:
		return true
	case BoolKind:
		return v.b == other.b
	case Int64Kind:
		return v.i == other.i
	case Float64Kind:
		return v.f == other.f
	case StringKind:
		return v.s == other.s
	case BinaryKind:
		return reflect.DeepEqual(v.bin, other.bin)
	case TimeKind:
		return v.t.Equal(other.t)
	case IDKind:
		return v.id == other.id
	case ArrayKind:
		if len(v.arr) != len(other.arr) {
			return false
		}
		for i := range v.arr {
			if !v.arr[i].Equal(other.arr[i]) {
				return false
			}
		}
		return true
	case ObjectKind:
		return v.obj.Equal(other.obj)
	default:
		return false
	}
}

// ValueOf converts a supported Go value into a database value with copied nested data.
func ValueOf(x any) (Value, error) {
	switch x := x.(type) {
	case nil:
		return Null(), nil
	case Value:
		return x.Clone(), nil
	case bool:
		return Bool(x), nil
	case string:
		return String(x), nil
	case []byte:
		return Binary(x), nil
	case time.Time:
		return Time(x), nil
	case DocumentID:
		return ID(x), nil
	case int:
		return Int(int64(x)), nil
	case int8:
		return Int(int64(x)), nil
	case int16:
		return Int(int64(x)), nil
	case int32:
		return Int(int64(x)), nil
	case int64:
		return Int(x), nil
	case uint:
		if uint64(x) <= math.MaxInt64 {
			return Int(int64(x)), nil
		}
	case uint8:
		return Int(int64(x)), nil
	case uint16:
		return Int(int64(x)), nil
	case uint32:
		return Int(int64(x)), nil
	case uint64:
		if x <= math.MaxInt64 {
			return Int(int64(x)), nil
		}
	case float32:
		return Float(float64(x)), nil
	case float64:
		return Float(x), nil
	case []Value:
		return Array(x...), nil
	case []any:
		values := make([]Value, len(x))
		for i := range x {
			v, err := ValueOf(x[i])
			if err != nil {
				return Value{}, err
			}
			values[i] = v
		}
		return Array(values...), nil
	case Document:
		return Object(x), nil
	case map[string]any:
		d, err := NewDocument(x)
		if err != nil {
			return Value{}, err
		}
		return Object(d), nil
	}
	return Value{}, fmt.Errorf("%w: unsupported value type %T", ErrInvalidDocument, x)
}

// compareNumeric compares an integer with a float without losing integer precision.
func compareNumeric(a, b Value) (int, bool) {
	if a.kind == Int64Kind && b.kind == Float64Kind {
		return compareIntFloat(a.i, b.f)
	}
	if a.kind == Float64Kind && b.kind == Int64Kind {
		c, ok := compareIntFloat(b.i, a.f)
		return -c, ok
	}
	return 0, false
}

// compareIntFloat orders an integer against a float and rejects NaN.
func compareIntFloat(i int64, f float64) (int, bool) {
	if math.IsNaN(f) {
		return 0, false
	}
	const two63 = float64(1 << 63)
	if f >= two63 {
		return -1, true
	}
	if f < -two63 {
		return 1, true
	}
	if float64(i) == f {
		// float64(i) may round. Equality requires the float to be integral and
		// convert back to the original integer exactly.
		if math.Trunc(f) == f && int64(f) == i {
			return 0, true
		}
	}
	if float64(i) < f {
		return -1, true
	}
	return 1, true
}

// cloneValues copies a slice and all nested data in its values.
func cloneValues(in []Value) []Value {
	if in == nil {
		return nil
	}
	out := make([]Value, len(in))
	for i := range in {
		out[i] = in[i].Clone()
	}
	return out
}
