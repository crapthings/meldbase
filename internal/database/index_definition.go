package database

import (
	"bytes"
	"fmt"
)

// newIndexDefinition creates an index definition from a nonempty list of copied fields.
func newIndexDefinition(name string, fields []IndexField, unique bool) IndexDefinition {
	cloned := cloneIndexFields(fields)
	return IndexDefinition{
		Name: name, Field: cloned[0].Field, Order: cloned[0].Order,
		Unique: unique, Fields: cloned,
	}
}

// cloneIndexFields copies the ordered field definitions.
func cloneIndexFields(fields []IndexField) []IndexField {
	if len(fields) == 0 {
		return nil
	}
	return append([]IndexField(nil), fields...)
}

// indexDefinitionFields returns the ordered fields, including legacy single-field definitions.
func indexDefinitionFields(definition IndexDefinition) []IndexField {
	if len(definition.Fields) > 0 {
		return definition.Fields
	}
	if definition.Field == "" {
		return nil
	}
	return []IndexField{{Field: definition.Field, Order: definition.Order}}
}

// cloneIndexDefinition copies an index definition and keeps its legacy fields in sync.
func cloneIndexDefinition(definition IndexDefinition) IndexDefinition {
	definition.Fields = cloneIndexFields(indexDefinitionFields(definition))
	if len(definition.Fields) > 0 {
		definition.Field, definition.Order = definition.Fields[0].Field, definition.Fields[0].Order
	}
	return definition
}

// equalIndexDefinitions compares index names, uniqueness, and ordered fields.
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

// usesCompoundIndexCodec reports whether an index needs tuple or descending-order encoding.
func usesCompoundIndexCodec(definition IndexDefinition) bool {
	fields := indexDefinitionFields(definition)
	return len(fields) != 1 || (len(fields) == 1 && fields[0].Order != 1)
}

// indexDocumentKey derives a full or partial index key and reports whether the document is indexed.
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

// compoundIndexQueryBounds derives a scan interval from leading equalities and the next field range.
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

// compoundIndexValueSpans builds scan intervals for allowed values of the first index field.
//
// Each value owns one contiguous prefix span; later conditions remain residual
// predicates unless the compound-bound planner proves a narrower span.
func compoundIndexValueSpans(definition IndexDefinition, expression expr) ([]indexScanSpan, bool) {
	fields := indexDefinitionFields(definition)
	if !usesCompoundIndexCodec(definition) || len(fields) == 0 {
		return nil, false
	}
	values, ok := indexValuesCandidate(expression, fields[0].Field)
	if !ok {
		return nil, false
	}
	spans := make([]indexScanSpan, 0, len(values))
	for _, value := range values {
		prefix, err := encodeCompoundIndexPrefix([]Value{value}, fields)
		if err != nil {
			return nil, false
		}
		end := indexKeyPrefixEnd(prefix)
		if end == nil {
			return nil, false
		}
		spans = append(spans, indexScanSpan{start: prefix, end: end})
	}
	return spans, true
}

// indexQueryScore ranks indexes by how many leading fields the query constrains.
//
// Each equality adds two points and a range adds one, favoring longer prefixes
// and exact matches.
func indexQueryScore(definition IndexDefinition, expression expr) int {
	fields := indexDefinitionFields(definition)
	if len(fields) == 0 {
		return 0
	}
	score := 0
	for _, field := range fields {
		if values, found := indexValuesCandidate(expression, field.Field); found {
			if len(values) == 0 {
				return score + 2
			}
			score += 2
			// A multi-value union is usable for a single-field index but cannot
			// form one contiguous compound-index prefix.
			if len(values) != 1 {
				break
			}
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
