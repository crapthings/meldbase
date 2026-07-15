package meldbase

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type wireQuerySpec struct {
	Version int             `json:"version"`
	Where   json.RawMessage `json:"where"`
	Sort    []SortField     `json:"sort,omitempty"`
	Skip    *int            `json:"skip,omitempty"`
	Limit   *int            `json:"limit,omitempty"`
}

type wireExpr struct {
	Op     string            `json:"op"`
	Args   []json.RawMessage `json:"args,omitempty"`
	Arg    json.RawMessage   `json:"arg,omitempty"`
	Cmp    string            `json:"cmp,omitempty"`
	Path   string            `json:"path,omitempty"`
	Value  json.RawMessage   `json:"value,omitempty"`
	Values []json.RawMessage `json:"values,omitempty"`
	Exists *bool             `json:"-"`
}

type wireValue struct {
	Type  string          `json:"t"`
	Value json.RawMessage `json:"v,omitempty"`
}

type queryDecoder struct {
	limits QueryLimits
	nodes  int
}

func DecodeQuerySpecJSON(data []byte, limits QueryLimits) (QuerySpec, error) {
	limits = normalizedLimits(limits)
	if len(data) > limits.MaxWireBytes {
		return QuerySpec{}, fmt.Errorf("%w: query exceeds wire limit", ErrInvalidFilter)
	}
	if err := validateJSON(data); err != nil {
		return QuerySpec{}, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}
	var wire wireQuerySpec
	if err := strictJSON(data, &wire); err != nil {
		return QuerySpec{}, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
	}
	if wire.Version != 1 {
		return QuerySpec{}, fmt.Errorf("%w: unsupported query version %d", ErrInvalidFilter, wire.Version)
	}
	if len(wire.Where) == 0 {
		return QuerySpec{}, fmt.Errorf("%w: missing where expression", ErrInvalidFilter)
	}
	if len(wire.Sort) > limits.MaxSortFields {
		return QuerySpec{}, fmt.Errorf("%w: too many sort fields", ErrInvalidFilter)
	}
	for _, field := range wire.Sort {
		if err := validatePath(field.Path); err != nil {
			return QuerySpec{}, err
		}
		if field.Direction != 1 && field.Direction != -1 {
			return QuerySpec{}, fmt.Errorf("%w: invalid sort direction", ErrInvalidFilter)
		}
	}
	skip := 0
	if wire.Skip != nil {
		if *wire.Skip < 0 {
			return QuerySpec{}, fmt.Errorf("%w: invalid skip", ErrInvalidFilter)
		}
		skip = *wire.Skip
	}
	var limit *int
	if wire.Limit != nil {
		if *wire.Limit < 0 || *wire.Limit > limits.MaxLimit {
			return QuerySpec{}, fmt.Errorf("%w: invalid limit", ErrInvalidFilter)
		}
		value := *wire.Limit
		limit = &value
	}
	decoder := queryDecoder{limits: limits}
	where, err := decoder.decodeExpr(wire.Where, 0)
	if err != nil {
		return QuerySpec{}, err
	}
	return QuerySpec{where: where, sort: append([]SortField(nil), wire.Sort...), skip: skip, limit: limit}, nil
}

// MarshalQuerySpecJSON emits the canonical, data-only wire representation used
// for transport fingerprints and cross-language conformance.
func MarshalQuerySpecJSON(query QuerySpec) ([]byte, error) {
	where, err := encodeQueryExpression(query.where)
	if err != nil {
		return nil, err
	}
	wire := map[string]any{"version": 1, "where": where}
	if len(query.sort) > 0 {
		wire["sort"] = query.Sort()
	}
	if query.skip != 0 {
		wire["skip"] = query.skip
	}
	if query.limit != nil {
		wire["limit"] = *query.limit
	}
	return json.Marshal(wire)
}

func encodeQueryExpression(expression expr) (any, error) {
	switch value := expression.(type) {
	case trueExpr:
		return map[string]any{"op": "true"}, nil
	case logicalExpr:
		args := make([]any, len(value.args))
		for index, child := range value.args {
			encoded, err := encodeQueryExpression(child)
			if err != nil {
				return nil, err
			}
			args[index] = encoded
		}
		return map[string]any{"op": value.op, "args": args}, nil
	case notExpr:
		arg, err := encodeQueryExpression(value.arg)
		if err != nil {
			return nil, err
		}
		return map[string]any{"op": "not", "arg": arg}, nil
	case compareExpr:
		encoded, err := encodeWireValue(value.value)
		if err != nil {
			return nil, err
		}
		return map[string]any{"op": "compare", "cmp": value.cmp, "path": value.path, "value": encoded}, nil
	case membershipExpr:
		values := make([]any, len(value.values))
		for index, item := range value.values {
			encoded, err := encodeWireValue(item)
			if err != nil {
				return nil, err
			}
			values[index] = encoded
		}
		return map[string]any{"op": value.op, "path": value.path, "values": values}, nil
	case existsExpr:
		return map[string]any{"op": "exists", "path": value.path, "value": value.value}, nil
	default:
		return nil, fmt.Errorf("%w: unknown compiled expression", ErrInvalidFilter)
	}
}

// ValidateStrictJSON rejects oversized, trailing, deeply nested, and
// duplicate-key JSON before a transport decodes it into structs or maps.
func ValidateStrictJSON(data []byte, maxBytes int) error {
	if maxBytes > 0 && len(data) > maxBytes {
		return errors.New("JSON exceeds size limit")
	}
	return validateJSON(data)
}

func normalizedLimits(limits QueryLimits) QueryLimits {
	defaults := DefaultQueryLimits
	if limits.MaxWireBytes <= 0 {
		limits.MaxWireBytes = defaults.MaxWireBytes
	}
	if limits.MaxDepth <= 0 {
		limits.MaxDepth = defaults.MaxDepth
	}
	if limits.MaxNodes <= 0 {
		limits.MaxNodes = defaults.MaxNodes
	}
	if limits.MaxArrayItems <= 0 {
		limits.MaxArrayItems = defaults.MaxArrayItems
	}
	if limits.MaxValueBytes <= 0 {
		limits.MaxValueBytes = defaults.MaxValueBytes
	}
	if limits.MaxSortFields <= 0 {
		limits.MaxSortFields = defaults.MaxSortFields
	}
	if limits.MaxLimit <= 0 {
		limits.MaxLimit = defaults.MaxLimit
	}
	return limits
}

func (d *queryDecoder) decodeExpr(data []byte, depth int) (expr, error) {
	if depth > d.limits.MaxDepth {
		return nil, fmt.Errorf("%w: query nesting is too deep", ErrInvalidFilter)
	}
	d.nodes++
	if d.nodes > d.limits.MaxNodes {
		return nil, fmt.Errorf("%w: too many query nodes", ErrInvalidFilter)
	}
	fields, err := rawObject(data)
	if err != nil {
		return nil, fmt.Errorf("%w: expression must be an object", ErrInvalidFilter)
	}
	opRaw, ok := fields["op"]
	if !ok {
		return nil, fmt.Errorf("%w: expression is missing op", ErrInvalidFilter)
	}
	var op string
	if err := strictJSON(opRaw, &op); err != nil {
		return nil, fmt.Errorf("%w: invalid op", ErrInvalidFilter)
	}
	allowed := map[string]map[string]bool{
		"true": {"op": true}, "and": {"op": true, "args": true}, "or": {"op": true, "args": true},
		"not": {"op": true, "arg": true}, "compare": {"op": true, "cmp": true, "path": true, "value": true},
		"in": {"op": true, "path": true, "values": true}, "nin": {"op": true, "path": true, "values": true},
		"exists": {"op": true, "path": true, "value": true},
	}
	permitted, known := allowed[op]
	if !known {
		return nil, fmt.Errorf("%w: unknown query op %q", ErrInvalidFilter, op)
	}
	for field := range fields {
		if !permitted[field] {
			return nil, fmt.Errorf("%w: unexpected %q for %s", ErrInvalidFilter, field, op)
		}
	}
	switch op {
	case "true":
		return trueExpr{}, nil
	case "and", "or":
		var args []json.RawMessage
		if err := requiredField(fields, "args", &args); err != nil {
			return nil, err
		}
		if len(args) == 0 || len(args) > d.limits.MaxArrayItems {
			return nil, fmt.Errorf("%w: %s args outside bounds", ErrInvalidFilter, op)
		}
		decoded := make([]expr, len(args))
		for i := range args {
			decoded[i], err = d.decodeExpr(args[i], depth+1)
			if err != nil {
				return nil, err
			}
		}
		return logicalExpr{op: op, args: decoded}, nil
	case "not":
		raw, ok := fields["arg"]
		if !ok {
			return nil, fmt.Errorf("%w: not is missing arg", ErrInvalidFilter)
		}
		arg, err := d.decodeExpr(raw, depth+1)
		if err != nil {
			return nil, err
		}
		return notExpr{arg: arg}, nil
	case "exists":
		path, err := decodePath(fields)
		if err != nil {
			return nil, err
		}
		var value bool
		if err := requiredField(fields, "value", &value); err != nil {
			return nil, err
		}
		return existsExpr{path: path, value: value}, nil
	case "in", "nin":
		path, err := decodePath(fields)
		if err != nil {
			return nil, err
		}
		var rawValues []json.RawMessage
		if err := requiredField(fields, "values", &rawValues); err != nil {
			return nil, err
		}
		if len(rawValues) > d.limits.MaxArrayItems {
			return nil, fmt.Errorf("%w: membership list too large", ErrInvalidFilter)
		}
		values := make([]Value, len(rawValues))
		for i := range rawValues {
			values[i], err = decodeWireValue(rawValues[i], d.limits, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return membershipExpr{op: op, path: path, values: values}, nil
	default:
		path, err := decodePath(fields)
		if err != nil {
			return nil, err
		}
		var cmp string
		if err := requiredField(fields, "cmp", &cmp); err != nil {
			return nil, err
		}
		if cmp != "eq" && cmp != "ne" && cmp != "gt" && cmp != "gte" && cmp != "lt" && cmp != "lte" {
			return nil, fmt.Errorf("%w: invalid comparison %q", ErrInvalidFilter, cmp)
		}
		raw, ok := fields["value"]
		if !ok {
			return nil, fmt.Errorf("%w: compare is missing value", ErrInvalidFilter)
		}
		value, err := decodeWireValue(raw, d.limits, depth+1)
		if err != nil {
			return nil, err
		}
		return compareExpr{cmp: cmp, path: path, value: value}, nil
	}
}

func decodePath(fields map[string]json.RawMessage) (string, error) {
	var path string
	if err := requiredField(fields, "path", &path); err != nil {
		return "", err
	}
	if err := validatePath(path); err != nil {
		return "", err
	}
	return path, nil
}

func decodeWireValue(data []byte, limits QueryLimits, depth int) (Value, error) {
	if depth > 64 {
		return Value{}, fmt.Errorf("%w: value nesting is too deep", ErrInvalidFilter)
	}
	fields, err := rawObject(data)
	if err != nil {
		return Value{}, fmt.Errorf("%w: malformed wire value", ErrInvalidFilter)
	}
	for field := range fields {
		if field != "t" && field != "v" {
			return Value{}, fmt.Errorf("%w: unexpected wire value field", ErrInvalidFilter)
		}
	}
	var kind string
	if err := requiredField(fields, "t", &kind); err != nil {
		return Value{}, err
	}
	raw, hasValue := fields["v"]
	if kind == "null" {
		if hasValue {
			return Value{}, fmt.Errorf("%w: null must not carry a value", ErrInvalidFilter)
		}
		return Null(), nil
	}
	if !hasValue {
		return Value{}, fmt.Errorf("%w: wire value is missing v", ErrInvalidFilter)
	}
	var value Value
	switch kind {
	case "bool":
		var x bool
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		value = Bool(x)
	case "number":
		var x float64
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		value = Float(x)
	case "int64":
		var x string
		if err := strictJSON(raw, &x); err != nil || !canonicalInteger(x) {
			return Value{}, invalidWire(err)
		}
		parsed, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return Value{}, invalidWire(err)
		}
		value = Int(parsed)
	case "string":
		var x string
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		value = String(x)
	case "date":
		var x string
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		parsed, err := time.Parse("2006-01-02T15:04:05.000Z", x)
		if err != nil {
			return Value{}, invalidWire(err)
		}
		value = Time(parsed)
	case "binary":
		var x string
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		parsed, err := base64.StdEncoding.Strict().DecodeString(x)
		if err != nil {
			return Value{}, invalidWire(err)
		}
		value = Binary(parsed)
	case "id":
		var x string
		if err := strictJSON(raw, &x); err != nil {
			return Value{}, invalidWire(err)
		}
		parsed, err := ParseDocumentID(x)
		if err != nil {
			return Value{}, invalidWire(err)
		}
		value = ID(parsed)
	case "array":
		var items []json.RawMessage
		if err := strictJSON(raw, &items); err != nil || len(items) > limits.MaxArrayItems {
			return Value{}, invalidWire(err)
		}
		values := make([]Value, len(items))
		for i := range items {
			values[i], err = decodeWireValue(items[i], limits, depth+1)
			if err != nil {
				return Value{}, err
			}
		}
		value = Array(values...)
	case "object":
		var entries []json.RawMessage
		if err := strictJSON(raw, &entries); err != nil || len(entries) > limits.MaxArrayItems {
			return Value{}, invalidWire(err)
		}
		document := make(Document, len(entries))
		for _, entry := range entries {
			var pair []json.RawMessage
			if err := strictJSON(entry, &pair); err != nil || len(pair) != 2 {
				return Value{}, invalidWire(err)
			}
			var key string
			if err := strictJSON(pair[0], &key); err != nil || validField(key) != nil {
				return Value{}, invalidWire(err)
			}
			if _, exists := document[key]; exists {
				return Value{}, fmt.Errorf("%w: duplicate object field %q", ErrInvalidFilter, key)
			}
			item, err := decodeWireValue(pair[1], limits, depth+1)
			if err != nil {
				return Value{}, err
			}
			document[key] = item
		}
		value = Object(document)
	default:
		return Value{}, fmt.Errorf("%w: unknown wire value type %q", ErrInvalidFilter, kind)
	}
	if wireValueBytes(value) > limits.MaxValueBytes {
		return Value{}, fmt.Errorf("%w: query value is too large", ErrInvalidFilter)
	}
	return value, nil
}

func MarshalWireValue(value Value) ([]byte, error) {
	encoded, err := encodeWireValue(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(encoded)
}

func MarshalWireDocument(document Document) ([]byte, error) {
	if err := document.Validate(); err != nil {
		return nil, err
	}
	return MarshalWireValue(Object(document))
}

func UnmarshalWireDocument(data []byte, limits QueryLimits) (Document, error) {
	return unmarshalWireDocument(data, limits, true)
}

func UnmarshalWireInputDocument(data []byte, limits QueryLimits) (Document, error) {
	return unmarshalWireDocument(data, limits, false)
}

func unmarshalWireDocument(data []byte, limits QueryLimits, requireID bool) (Document, error) {
	limits = normalizedLimits(limits)
	if len(data) > limits.MaxWireBytes {
		return nil, fmt.Errorf("%w: document exceeds wire limit", ErrInvalidDocument)
	}
	if err := validateJSON(data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDocument, err)
	}
	value, err := decodeWireValue(data, limits, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDocument, err)
	}
	if value.kind != ObjectKind {
		return nil, fmt.Errorf("%w: wire document is not object", ErrInvalidDocument)
	}
	if _, exists := value.obj["_id"]; exists {
		if _, ok := value.obj.ID(); !ok {
			return nil, fmt.Errorf("%w: wire document requires typed _id", ErrInvalidDocument)
		}
	} else if requireID {
		return nil, fmt.Errorf("%w: wire document requires typed _id", ErrInvalidDocument)
	}
	return value.obj.Clone(), nil
}

func encodeWireValue(value Value) (any, error) {
	switch value.kind {
	case NullKind:
		return map[string]any{"t": "null"}, nil
	case BoolKind:
		return map[string]any{"t": "bool", "v": value.b}, nil
	case Int64Kind:
		return map[string]any{"t": "int64", "v": strconv.FormatInt(value.i, 10)}, nil
	case Float64Kind:
		if err := validateArray([]Value{value}, 0); err != nil {
			return nil, err
		}
		return map[string]any{"t": "number", "v": value.f}, nil
	case StringKind:
		return map[string]any{"t": "string", "v": value.s}, nil
	case BinaryKind:
		return map[string]any{"t": "binary", "v": base64.StdEncoding.EncodeToString(value.bin)}, nil
	case TimeKind:
		return map[string]any{"t": "date", "v": value.t.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")}, nil
	case ArrayKind:
		items := make([]any, len(value.arr))
		for i := range value.arr {
			encoded, err := encodeWireValue(value.arr[i])
			if err != nil {
				return nil, err
			}
			items[i] = encoded
		}
		return map[string]any{"t": "array", "v": items}, nil
	case ObjectKind:
		if err := value.obj.Validate(); err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(value.obj))
		for key := range value.obj {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		entries := make([]any, len(keys))
		for i, key := range keys {
			encoded, err := encodeWireValue(value.obj[key])
			if err != nil {
				return nil, err
			}
			entries[i] = []any{key, encoded}
		}
		return map[string]any{"t": "object", "v": entries}, nil
	case IDKind:
		return map[string]any{"t": "id", "v": value.id.String()}, nil
	default:
		return nil, fmt.Errorf("%w: unknown value kind", ErrInvalidDocument)
	}
}

func wireValueBytes(v Value) int {
	switch v.kind {
	case NullKind, BoolKind:
		return 1
	case Int64Kind, Float64Kind, TimeKind:
		return 8
	case IDKind:
		return 16
	case StringKind:
		return len(v.s)
	case BinaryKind:
		return len(v.bin)
	}
	if v.kind == ArrayKind {
		total := 0
		for _, item := range v.arr {
			total += wireValueBytes(item)
		}
		return total
	}
	total := 0
	for key, item := range v.obj {
		total += len(key) + wireValueBytes(item)
	}
	return total
}

func requiredField[T any](fields map[string]json.RawMessage, name string, target *T) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("%w: missing %s", ErrInvalidFilter, name)
	}
	if err := strictJSON(raw, target); err != nil {
		return fmt.Errorf("%w: invalid %s", ErrInvalidFilter, name)
	}
	return nil
}

func rawObject(data []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := strictJSON(data, &fields); err != nil || fields == nil {
		return nil, errors.New("not an object")
	}
	return fields, nil
}
func strictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
func invalidWire(err error) error {
	if err == nil {
		return fmt.Errorf("%w: malformed wire value", ErrInvalidFilter)
	}
	return fmt.Errorf("%w: malformed wire value: %v", ErrInvalidFilter, err)
}
func canonicalInteger(value string) bool {
	if value == "0" {
		return true
	}
	if strings.HasPrefix(value, "-") {
		value = value[1:]
	}
	return len(value) > 0 && value[0] >= '1' && value[0] <= '9' && allDigits(value)
}
func allDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 128 {
		return errors.New("JSON nesting is too deep")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		if text, ok := token.(string); ok && !utf8.ValidString(text) {
			return errors.New("invalid UTF-8")
		}
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("malformed object")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("malformed array")
		}
	default:
		return errors.New("unexpected delimiter")
	}
	return nil
}
