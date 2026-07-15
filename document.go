package meldbase

import (
	"fmt"
	"math"
	"strings"
)

type Document map[string]Value

func NewDocument(fields map[string]any) (Document, error) {
	d := make(Document, len(fields))
	for k, raw := range fields {
		if err := validField(k); err != nil {
			return nil, err
		}
		v, err := ValueOf(raw)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", k, err)
		}
		d[k] = v
	}
	return d, nil
}

func (d Document) Clone() Document {
	if d == nil {
		return nil
	}
	out := make(Document, len(d))
	for k, v := range d {
		out[k] = v.Clone()
	}
	return out
}

func (d Document) Equal(other Document) bool {
	if len(d) != len(other) {
		return false
	}
	for k, v := range d {
		ov, ok := other[k]
		if !ok || !v.Equal(ov) {
			return false
		}
	}
	return true
}

func (d Document) ID() (DocumentID, bool) {
	v, ok := d["_id"]
	if !ok {
		return DocumentID{}, false
	}
	return v.IDValue()
}

func lookup(d Document, path string) (Value, bool) {
	parts := strings.Split(path, ".")
	current := d
	for i, part := range parts {
		v, ok := current[part]
		if !ok {
			return Value{}, false
		}
		if i == len(parts)-1 {
			return v, true
		}
		var isObj bool
		current, isObj = v.ObjectValue()
		if !isObj {
			return Value{}, false
		}
	}
	return Value{}, false
}

func validField(k string) error {
	if k == "" || strings.ContainsRune(k, '\x00') || strings.Contains(k, ".") || strings.HasPrefix(k, "$") || k == "__proto__" || k == "prototype" || k == "constructor" {
		return fmt.Errorf("%w: invalid field name %q", ErrInvalidDocument, k)
	}
	return nil
}

// Validate checks every nested field and value before a document crosses a
// storage or transport boundary.
func (d Document) Validate() error {
	return validateDocument(d, 0)
}

func validateDocument(d Document, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%w: document nesting is too deep", ErrInvalidDocument)
	}
	for k, v := range d {
		if err := validField(k); err != nil {
			return err
		}
		if v.kind == Float64Kind && (math.IsNaN(v.f) || math.IsInf(v.f, 0)) {
			return fmt.Errorf("%w: non-finite number at %q", ErrInvalidDocument, k)
		}
		if v.kind == ObjectKind {
			if err := validateDocument(v.obj, depth+1); err != nil {
				return err
			}
		}
		if v.kind == ArrayKind {
			if err := validateArray(v.arr, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateArray(values []Value, depth int) error {
	if depth > 64 {
		return fmt.Errorf("%w: document nesting is too deep", ErrInvalidDocument)
	}
	for _, v := range values {
		if v.kind == Float64Kind && (math.IsNaN(v.f) || math.IsInf(v.f, 0)) {
			return fmt.Errorf("%w: non-finite number", ErrInvalidDocument)
		}
		if v.kind == ObjectKind {
			if err := validateDocument(v.obj, depth+1); err != nil {
				return err
			}
		}
		if v.kind == ArrayKind {
			if err := validateArray(v.arr, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
