package admin

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	meldserver "github.com/crapthings/meldbase/server"
)

type adminWireSchemaField struct {
	Path     string `json:"path"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
	Nullable bool   `json:"nullable"`
}

func TestAdminSampleWireSchemaMatchesJSONEncoder(t *testing.T) {
	fields := buildAdminWireSchema(t)
	sample := Sample{Server: &meldserver.ServerStats{}}
	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	actual := make(map[string]string, len(fields))
	collectAdminJSONPaths(t, decoded, "", actual)
	for _, field := range fields {
		wireType, exists := actual[field.Path]
		if !exists {
			t.Fatalf("schema path %q is absent from populated JSON", field.Path)
		}
		if !adminSchemaTypeMatchesJSON(field.Type, wireType) {
			t.Fatalf("schema path %q type=%q JSON=%q", field.Path, field.Type, wireType)
		}
		delete(actual, field.Path)
	}
	if len(actual) != 0 {
		t.Fatalf("JSON encoder contains paths absent from schema: %+v", actual)
	}

	sample.Server = nil
	encoded, err = json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"server"`) {
		t.Fatalf("optional server object was not omitted: %s", encoded)
	}
}

func buildAdminWireSchema(t *testing.T) []adminWireSchemaField {
	t.Helper()
	fields := make([]adminWireSchemaField, 0, 256)
	collectAdminWireSchema(t, reflect.TypeOf(Sample{}), "", false, &fields)
	sort.Slice(fields, func(left, right int) bool { return fields[left].Path < fields[right].Path })
	for index := 1; index < len(fields); index++ {
		if fields[index-1].Path == fields[index].Path {
			t.Fatalf("duplicate admin schema path %q", fields[index].Path)
		}
	}
	return fields
}

func collectAdminWireSchema(t *testing.T, value reflect.Type, prefix string, inheritedOptional bool, fields *[]adminWireSchemaField) {
	t.Helper()
	if value.Kind() != reflect.Struct {
		t.Fatalf("admin schema root %s is not a struct", value)
	}
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		if field.PkgPath != "" {
			continue
		}
		name, optional, included := adminJSONField(field)
		if !included {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		optional = optional || inheritedOptional
		fieldType := field.Type
		nullable := false
		if fieldType.Kind() == reflect.Pointer {
			nullable = true
			fieldType = fieldType.Elem()
		}
		if fieldType != reflect.TypeOf(time.Time{}) && fieldType.Kind() == reflect.Struct {
			*fields = append(*fields, adminWireSchemaField{Path: path, Type: "object", Optional: optional, Nullable: nullable})
			collectAdminWireSchema(t, fieldType, path, optional || nullable, fields)
			continue
		}
		*fields = append(*fields, adminWireSchemaField{Path: path, Type: adminJSONWireType(t, fieldType), Optional: optional, Nullable: nullable})
	}
}

func adminJSONField(field reflect.StructField) (name string, optional, included bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	if len(parts) > 0 && parts[0] == "-" {
		return "", false, false
	}
	name = field.Name
	if len(parts) > 0 && parts[0] != "" {
		name = parts[0]
	}
	for _, option := range parts[1:] {
		optional = optional || option == "omitempty" || option == "omitzero"
	}
	return name, optional, true
}

func adminJSONWireType(t *testing.T, value reflect.Type) string {
	t.Helper()
	if value == reflect.TypeOf(time.Time{}) {
		return "string:rfc3339nano"
	}
	if value == reflect.TypeOf(time.Duration(0)) {
		return "integer:nanoseconds"
	}
	switch value.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "string"
	case reflect.Float32:
		return "number:float32"
	case reflect.Float64:
		return "number:float64"
	case reflect.Int:
		return "integer:int"
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("integer:int%d", value.Bits())
	case reflect.Uint:
		return "integer:uint"
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("integer:uint%d", value.Bits())
	case reflect.Slice, reflect.Array:
		return "array<" + adminJSONWireType(t, value.Elem()) + ">"
	default:
		t.Fatalf("unsupported admin JSON field type %s", value)
		return ""
	}
}

func collectAdminJSONPaths(t *testing.T, value map[string]any, prefix string, paths map[string]string) {
	t.Helper()
	for name, raw := range value {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		switch typed := raw.(type) {
		case map[string]any:
			paths[path] = "object"
			collectAdminJSONPaths(t, typed, path, paths)
		case bool:
			paths[path] = "boolean"
		case string:
			paths[path] = "string"
		case float64:
			paths[path] = "number"
		case nil:
			paths[path] = "null"
		case []any:
			paths[path] = "array"
		default:
			t.Fatalf("unsupported encoded JSON type %T at %s", raw, path)
		}
	}
}

func adminSchemaTypeMatchesJSON(schemaType, jsonType string) bool {
	switch {
	case schemaType == "object":
		return jsonType == "object"
	case schemaType == "boolean":
		return jsonType == "boolean"
	case schemaType == "string" || strings.HasPrefix(schemaType, "string:"):
		return jsonType == "string"
	case strings.HasPrefix(schemaType, "integer:") || strings.HasPrefix(schemaType, "number:"):
		return jsonType == "number"
	case strings.HasPrefix(schemaType, "array<"):
		return jsonType == "array"
	default:
		return false
	}
}
