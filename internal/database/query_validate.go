package database

import (
	"fmt"
	"math"
	"unicode/utf8"
)

// validateSortFields rejects excessive, duplicate, or invalid sort fields.
func validateSortFields(fields []SortField, limits QueryLimits) error {
	if len(fields) > limits.MaxSortFields {
		return fmt.Errorf("%w: too many sort fields", ErrInvalidFilter)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := validatePath(field.Path); err != nil {
			return err
		}
		if field.Direction != 1 && field.Direction != -1 {
			return fmt.Errorf("%w: invalid sort direction", ErrInvalidFilter)
		}
		if _, duplicate := seen[field.Path]; duplicate {
			return fmt.Errorf("%w: duplicate sort path %q", ErrInvalidFilter, field.Path)
		}
		seen[field.Path] = struct{}{}
	}
	return nil
}

// querySpecValidator tracks expression counts while checking query input limits.
type querySpecValidator struct {
	limits QueryLimits
	nodes  int
}

// validateQuerySpec checks compiled query structure, operands, modifiers, and encoded size.
func validateQuerySpec(query QuerySpec, limits QueryLimits) error {
	limits = normalizedLimits(limits)
	if query.where == nil {
		return fmt.Errorf("%w: missing where expression", ErrInvalidFilter)
	}
	if err := validateSortFields(query.sort, limits); err != nil {
		return err
	}
	if query.skip < 0 {
		return fmt.Errorf("%w: invalid skip", ErrInvalidFilter)
	}
	if query.limit != nil && (*query.limit < 0 || *query.limit > limits.MaxLimit) {
		return fmt.Errorf("%w: invalid limit", ErrInvalidFilter)
	}
	if query.seek && (len(query.sort) == 0 || query.limit == nil) {
		return fmt.Errorf("%w: seek pagination requires sort and limit", ErrInvalidFilter)
	}
	validator := querySpecValidator{limits: limits}
	if err := validator.expression(query.where, 0); err != nil {
		return err
	}
	encoded, err := marshalQuerySpecJSONUnchecked(query)
	if err != nil {
		return err
	}
	if len(encoded) > limits.MaxWireBytes {
		return fmt.Errorf("%w: query exceeds wire limit", ErrInvalidFilter)
	}
	return nil
}

// expression validates a document predicate and its nested conditions within the query limits.
func (v *querySpecValidator) expression(expression expr, depth int) error {
	if depth > v.limits.MaxDepth {
		return fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	v.nodes++
	if v.nodes > v.limits.MaxNodes {
		return fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	switch value := expression.(type) {
	case trueExpr:
		return nil
	case logicalExpr:
		if (value.op != "and" && value.op != "or") || len(value.args) == 0 || len(value.args) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid logical expression", ErrInvalidFilter)
		}
		for _, child := range value.args {
			if child == nil {
				return fmt.Errorf("%w: missing logical expression", ErrInvalidFilter)
			}
			if err := v.expression(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case notExpr:
		if value.arg == nil {
			return fmt.Errorf("%w: missing not expression", ErrInvalidFilter)
		}
		return v.expression(value.arg, depth+1)
	case compareExpr:
		if value.cmp != "eq" && value.cmp != "ne" && value.cmp != "gt" && value.cmp != "gte" && value.cmp != "lt" && value.cmp != "lte" {
			return fmt.Errorf("%w: invalid comparison %q", ErrInvalidFilter, value.cmp)
		}
		return v.operand(value.path, value.value)
	case membershipExpr:
		if (value.op != "in" && value.op != "nin") || len(value.values) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid membership expression", ErrInvalidFilter)
		}
		for _, item := range value.values {
			if err := v.operand(value.path, item); err != nil {
				return err
			}
		}
		return nil
	case existsExpr:
		return validatePath(value.path)
	case sizeExpr:
		if err := validatePath(value.path); err != nil {
			return err
		}
		if value.size < 0 || value.size > maxQueryArraySize {
			return fmt.Errorf("%w: invalid array size", ErrInvalidFilter)
		}
		return nil
	case typeExpr:
		if err := validatePath(value.path); err != nil {
			return err
		}
		if len(value.types) == 0 || len(value.types) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid type expression", ErrInvalidFilter)
		}
		normalized, err := normalizeQueryTypes(queryTypeNames(value.types), v.limits.MaxArrayItems)
		if err != nil || len(normalized) != len(value.types) {
			return fmt.Errorf("%w: invalid type expression", ErrInvalidFilter)
		}
		for index := range normalized {
			if normalized[index] != value.types[index] {
				return fmt.Errorf("%w: non-canonical type expression", ErrInvalidFilter)
			}
		}
		return nil
	case allExpr:
		if err := validatePath(value.path); err != nil {
			return err
		}
		if len(value.values) == 0 || len(value.values) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid all expression", ErrInvalidFilter)
		}
		for index, item := range value.values {
			if err := v.operand(value.path, item); err != nil {
				return err
			}
			if containsQueryValue(value.values[:index], item) {
				return fmt.Errorf("%w: non-canonical all expression", ErrInvalidFilter)
			}
		}
		return nil
	case elemMatchExpr:
		if err := validatePath(value.path); err != nil {
			return err
		}
		switch value.mode {
		case "scalar":
			if value.scalar == nil || value.object != nil {
				return fmt.Errorf("%w: invalid scalar elem match", ErrInvalidFilter)
			}
			return v.elementExpression(value.scalar, depth+1)
		case "object":
			if value.object == nil || value.scalar != nil {
				return fmt.Errorf("%w: invalid object elem match", ErrInvalidFilter)
			}
			return v.expression(value.object, depth+1)
		default:
			return fmt.Errorf("%w: invalid elem match mode", ErrInvalidFilter)
		}
	default:
		return fmt.Errorf("%w: unknown compiled expression", ErrInvalidFilter)
	}
}

// elementExpression validates scalar array-element conditions within the query limits.
func (v *querySpecValidator) elementExpression(expression elementExpr, depth int) error {
	if depth > v.limits.MaxDepth {
		return fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	v.nodes++
	if v.nodes > v.limits.MaxNodes {
		return fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	switch value := expression.(type) {
	case elementLogicalExpr:
		if (value.op != "and" && value.op != "or") || len(value.args) == 0 || len(value.args) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid scalar elem match logic", ErrInvalidFilter)
		}
		for _, child := range value.args {
			if child == nil {
				return fmt.Errorf("%w: missing scalar elem match condition", ErrInvalidFilter)
			}
			if err := v.elementExpression(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case elementNotExpr:
		if value.arg == nil {
			return fmt.Errorf("%w: missing scalar elem match condition", ErrInvalidFilter)
		}
		return v.elementExpression(value.arg, depth+1)
	case elementCompareExpr:
		if value.cmp != "eq" && value.cmp != "ne" && value.cmp != "gt" && value.cmp != "gte" && value.cmp != "lt" && value.cmp != "lte" {
			return fmt.Errorf("%w: invalid scalar elem match comparison", ErrInvalidFilter)
		}
		return v.value(value.value, 0)
	case elementMembershipExpr:
		if (value.op != "in" && value.op != "nin") || len(value.values) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: invalid scalar elem match membership", ErrInvalidFilter)
		}
		for _, item := range value.values {
			if err := v.value(item, 0); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown scalar elem match condition", ErrInvalidFilter)
	}
}

// operand validates a predicate's field path, value type, and wire size.
func (v *querySpecValidator) operand(path string, value Value) error {
	if err := validatePath(path); err != nil {
		return err
	}
	if path == "_id" && value.kind != IDKind {
		return fmt.Errorf("%w: _id requires a document id value", ErrInvalidFilter)
	}
	if err := v.value(value, 0); err != nil {
		return err
	}
	if wireValueBytes(value) > v.limits.MaxValueBytes {
		return fmt.Errorf("%w: query value is too large", ErrInvalidFilter)
	}
	return nil
}

// value validates nested query values against type and size limits.
func (v *querySpecValidator) value(value Value, depth int) error {
	if depth > v.limits.MaxDepth {
		return fmt.Errorf("%w: query value nesting is too deep", ErrInvalidFilter)
	}
	switch value.kind {
	case Float64Kind:
		if math.IsNaN(value.f) || math.IsInf(value.f, 0) {
			return fmt.Errorf("%w: non-finite query number", ErrInvalidFilter)
		}
	case StringKind:
		if !utf8.ValidString(value.s) {
			return fmt.Errorf("%w: invalid UTF-8 query string", ErrInvalidFilter)
		}
	case ArrayKind:
		if len(value.arr) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: query array has too many items", ErrInvalidFilter)
		}
		for _, item := range value.arr {
			if err := v.value(item, depth+1); err != nil {
				return err
			}
		}
	case ObjectKind:
		if len(value.obj) > v.limits.MaxArrayItems {
			return fmt.Errorf("%w: query object has too many fields", ErrInvalidFilter)
		}
		for key, item := range value.obj {
			if err := validField(key); err != nil {
				return fmt.Errorf("%w: invalid query object field", ErrInvalidFilter)
			}
			if err := v.value(item, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}
