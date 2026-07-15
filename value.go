package meldbase

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"time"
)

type Kind uint8

const (
	NullKind Kind = iota
	BoolKind
	Int64Kind
	Float64Kind
	StringKind
	BinaryKind
	TimeKind
	ArrayKind
	ObjectKind
	IDKind
)

type DocumentID [16]byte

func NewDocumentID() (DocumentID, error) {
	var id DocumentID
	_, err := rand.Read(id[:])
	return id, err
}

func ParseDocumentID(s string) (DocumentID, error) {
	var id DocumentID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, fmt.Errorf("%w: malformed document id", ErrInvalidDocument)
	}
	copy(id[:], b)
	return id, nil
}

func (id DocumentID) String() string { return hex.EncodeToString(id[:]) }
func (id DocumentID) IsZero() bool   { return id == DocumentID{} }

// Value is a closed tagged value. Its representation is private so callers
// cannot construct a tag/payload mismatch.
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

func Null() Value           { return Value{kind: NullKind} }
func Bool(v bool) Value     { return Value{kind: BoolKind, b: v} }
func Int(v int64) Value     { return Value{kind: Int64Kind, i: v} }
func Float(v float64) Value { return Value{kind: Float64Kind, f: v} }
func String(v string) Value { return Value{kind: StringKind, s: v} }
func Binary(v []byte) Value { return Value{kind: BinaryKind, bin: append([]byte(nil), v...)} }

// Time stores millisecond precision, matching JavaScript Date and the wire
// contract. Precision is normalized at construction rather than silently lost
// during transport.
func Time(v time.Time) Value {
	return Value{kind: TimeKind, t: v.Round(0).UTC().Truncate(time.Millisecond)}
}
func ID(v DocumentID) Value                  { return Value{kind: IDKind, id: v} }
func Array(v ...Value) Value                 { return Value{kind: ArrayKind, arr: cloneValues(v)} }
func Object(v Document) Value                { return Value{kind: ObjectKind, obj: v.Clone()} }
func (v Value) Kind() Kind                   { return v.kind }
func (v Value) Bool() (bool, bool)           { return v.b, v.kind == BoolKind }
func (v Value) Int64() (int64, bool)         { return v.i, v.kind == Int64Kind }
func (v Value) Float64() (float64, bool)     { return v.f, v.kind == Float64Kind }
func (v Value) StringValue() (string, bool)  { return v.s, v.kind == StringKind }
func (v Value) TimeValue() (time.Time, bool) { return v.t, v.kind == TimeKind }
func (v Value) IDValue() (DocumentID, bool)  { return v.id, v.kind == IDKind }
func (v Value) BinaryValue() ([]byte, bool) {
	return append([]byte(nil), v.bin...), v.kind == BinaryKind
}
func (v Value) ArrayValue() ([]Value, bool)   { return cloneValues(v.arr), v.kind == ArrayKind }
func (v Value) ObjectValue() (Document, bool) { return v.obj.Clone(), v.kind == ObjectKind }

func (v Value) Clone() Value {
	v.bin = append([]byte(nil), v.bin...)
	v.arr = cloneValues(v.arr)
	v.obj = v.obj.Clone()
	return v
}

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
