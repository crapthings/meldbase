package meldbase

import (
	"bytes"
	"fmt"
)

func newIndexDefinition(name string, fields []IndexField, unique bool) IndexDefinition {
	cloned := cloneIndexFields(fields)
	return IndexDefinition{
		Name: name, Field: cloned[0].Field, Order: cloned[0].Order,
		Unique: unique, Fields: cloned,
	}
}

func cloneIndexFields(fields []IndexField) []IndexField {
	if len(fields) == 0 {
		return nil
	}
	return append([]IndexField(nil), fields...)
}

func indexDefinitionFields(definition IndexDefinition) []IndexField {
	if len(definition.Fields) > 0 {
		return definition.Fields
	}
	if definition.Field == "" {
		return nil
	}
	return []IndexField{{Field: definition.Field, Order: definition.Order}}
}

func cloneIndexDefinition(definition IndexDefinition) IndexDefinition {
	definition.Fields = cloneIndexFields(indexDefinitionFields(definition))
	if len(definition.Fields) > 0 {
		definition.Field, definition.Order = definition.Fields[0].Field, definition.Fields[0].Order
	}
	return definition
}

func equalIndexDefinitions(left, right IndexDefinition) bool {
	if left.Name != right.Name || left.Unique != right.Unique {
		return false
	}
	leftFields, rightFields := indexDefinitionFields(left), indexDefinitionFields(right)
	if len(leftFields) != len(rightFields) {
		return false
	}
	for index := range leftFields {
		if leftFields[index] != rightFields[index] {
			return false
		}
	}
	return true
}

func usesCompoundIndexCodec(definition IndexDefinition) bool {
	fields := indexDefinitionFields(definition)
	return len(fields) != 1 || (len(fields) == 1 && fields[0].Order != 1)
}

func indexDocumentKey(definition IndexDefinition, document Document) ([]byte, bool, error) {
	fields := indexDefinitionFields(definition)
	if len(fields) == 0 {
		return nil, false, ErrInvalidIndex
	}
	values := make([]Value, len(fields))
	for index, field := range fields {
		value, found := lookupInternal(document, field.Field)
		if !found {
			if index == 0 || !usesCompoundIndexCodec(definition) {
				return nil, false, nil
			}
			id, exists := document.ID()
			if !exists || id.IsZero() {
				return nil, true, ErrInvalidIndex
			}
			key, err := encodeCompoundPartialIndexKey(values[:index], fields[:index], id)
			if err != nil {
				return nil, true, fmt.Errorf("%w: indexed tuple is invalid", ErrInvalidIndex)
			}
			return key, true, nil
		}
		values[index] = value
	}
	var (
		key []byte
		err error
	)
	if usesCompoundIndexCodec(definition) {
		key, err = encodeCompoundIndexKey(values, fields)
	} else {
		key, err = encodeIndexKey(values[0])
	}
	if err != nil {
		return nil, true, fmt.Errorf("%w: indexed field is not scalar", ErrInvalidIndex)
	}
	return key, true, nil
}

func compoundIndexQueryBounds(definition IndexDefinition, expression expr) (start, end []byte, ok, exact bool) {
	fields := indexDefinitionFields(definition)
	if !usesCompoundIndexCodec(definition) || len(fields) == 0 {
		return nil, nil, false, false
	}
	equalities := make([]Value, 0, len(fields))
	for _, field := range fields {
		value, found := equalityCandidate(expression, field.Field)
		if !found {
			break
		}
		if _, err := encodeIndexKey(value); err != nil {
			return nil, nil, false, false
		}
		equalities = append(equalities, value)
	}
	if len(equalities) == len(fields) {
		key, err := encodeCompoundIndexKey(equalities, fields)
		if err != nil {
			return nil, nil, false, false
		}
		return key, indexKeyPrefixEnd(key), true, true
	}
	prefix := []byte(nil)
	if len(equalities) > 0 {
		var err error
		prefix, err = encodeCompoundIndexPrefix(equalities, fields)
		if err != nil {
			return nil, nil, false, false
		}
	}
	next := fields[len(equalities)]
	lower, upper, hasRange := rangeCandidate(expression, next.Field)
	if !hasRange {
		if len(prefix) == 0 {
			return nil, nil, false, false
		}
		return prefix, indexKeyPrefixEnd(prefix), true, false
	}
	physicalLower, physicalUpper := lower, upper
	if next.Order == -1 {
		physicalLower, physicalUpper = upper, lower
	}
	// A range-constrained component cannot match a partial tuple. Every real
	// scalar component begins above compoundComponentFloor, while partial keys
	// begin with compoundPartialMarker.
	start = append(append([]byte(nil), prefix...), compoundComponentFloor)
	if physicalLower != nil {
		component, err := encodeCompoundIndexKey([]Value{physicalLower.value}, []IndexField{next})
		if err != nil {
			return nil, nil, false, false
		}
		start = append(append([]byte(nil), prefix...), component...)
		if !physicalLower.inclusive {
			start = indexKeyPrefixEnd(start)
			if start == nil {
				return nil, nil, true, false
			}
		}
	}
	end = indexKeyPrefixEnd(prefix)
	if physicalUpper != nil {
		component, err := encodeCompoundIndexKey([]Value{physicalUpper.value}, []IndexField{next})
		if err != nil {
			return nil, nil, false, false
		}
		end = append(append([]byte(nil), prefix...), component...)
		if physicalUpper.inclusive {
			end = indexKeyPrefixEnd(end)
		}
	}
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil, nil, true, false
	}
	return start, end, true, false
}

// indexQueryScore ranks usable indexes by the constrained left prefix. An
// equality component contributes two points and one range component contributes
// one, so a longer compound prefix wins while exact matches beat ranges.
func indexQueryScore(definition IndexDefinition, expression expr) int {
	fields := indexDefinitionFields(definition)
	if len(fields) == 0 {
		return 0
	}
	score := 0
	for _, field := range fields {
		if value, found := equalityCandidate(expression, field.Field); found {
			if _, err := encodeIndexKey(value); err != nil {
				return 0
			}
			score += 2
			continue
		}
		if lower, upper, found := rangeCandidate(expression, field.Field); found {
			if lower != nil {
				if _, err := encodeIndexKey(lower.value); err != nil {
					return 0
				}
			}
			if upper != nil {
				if _, err := encodeIndexKey(upper.value); err != nil {
					return 0
				}
			}
			score++
		}
		break
	}
	return score
}
