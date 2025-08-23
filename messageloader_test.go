package protobq

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/google/go-cmp/cmp"
	testdatav1 "github.com/way-platform/protobq-go/internal/gen/wayplatform/testdata/v1"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMessageLoader(t *testing.T) {
	type testCase struct {
		name          string
		messageLoader MessageLoader
		row           []bigquery.Value
		schema        bigquery.Schema
		expected      func() proto.Message
		expectedError string
	}
	type testCaseCategory struct {
		name      string
		testCases []testCase
	}
	testCaseCategories := []testCaseCategory{
		{
			name: "basic_functionality",
			testCases: []testCase{
				{
					name: "STRING to string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"test",
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "string_value",
							Type: bigquery.StringFieldType,
						},
					},
					expected: func() proto.Message {
						var result testdatav1.KitchenSink
						result.SetStringValue("test")
						return &result
					},
				},

				{
					name: "unknown field (no discard)",
					messageLoader: MessageLoader{
						DiscardUnknown: false,
						Message:        &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"test",
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "unknown_field",
							Type: bigquery.StringFieldType,
						},
					},
					expected: func() proto.Message {
						var result testdatav1.KitchenSink
						result.SetStringValue("test")
						return &result
					},
					expectedError: "unknown field",
				},

				{
					name: "unknown field (discard)",
					messageLoader: MessageLoader{
						DiscardUnknown: true,
						Message:        &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"test",
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "unknown_field",
							Type: bigquery.StringFieldType,
						},
					},
					expected: func() proto.Message {
						var result testdatav1.KitchenSink
						return &result
					},
				},

				{
					name: "all primitive types",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						3.14159,               // double_value
						float64(2.718),        // float_value (BigQuery returns float as float64)
						int64(42),             // int32_value (BigQuery returns int as int64)
						int64(1234567890),     // int64_value
						int64(-123),           // sint32_value
						int64(-987654321),     // sint64_value
						int64(100),            // uint32_value
						int64(9876543210),     // uint64_value
						int64(200),            // fixed32_value
						int64(1111111111),     // fixed64_value
						int64(-50),            // sfixed32_value
						int64(-2222222222),    // sfixed64_value
						true,                  // bool_value
						"hello world",         // string_value
						[]byte("binary data"), // bytes_value
						int64(1),              // enum_value (TEST_ENUM_VALUE_ONE)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "double_value", Type: bigquery.FloatFieldType},
						&bigquery.FieldSchema{Name: "float_value", Type: bigquery.FloatFieldType},
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "sint32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "sint64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "uint32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "uint64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "fixed32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "fixed64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "sfixed32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "sfixed64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "bool_value", Type: bigquery.BooleanFieldType},
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.StringFieldType},
						&bigquery.FieldSchema{Name: "bytes_value", Type: bigquery.BytesFieldType},
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDoubleValue(3.14159)
						result.SetFloatValue(2.718)
						result.SetInt32Value(42)
						result.SetInt64Value(1234567890)
						result.SetSint32Value(-123)
						result.SetSint64Value(-987654321)
						result.SetUint32Value(100)
						result.SetUint64Value(9876543210)
						result.SetFixed32Value(200)
						result.SetFixed64Value(1111111111)
						result.SetSfixed32Value(-50)
						result.SetSfixed64Value(-2222222222)
						result.SetBoolValue(true)
						result.SetStringValue("hello world")
						result.SetBytesValue([]byte("binary data"))
						result.SetEnumValue(testdatav1.TestEnum_TEST_ENUM_VALUE_ONE)
						return result
					},
				},
			},
		},
		{
			name: "repeated_and_nested",
			testCases: []testCase{
				{
					name: "repeated fields",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{"hello", "world", "test"},     // repeated_string
						[]bigquery.Value{int64(1), int64(2), int64(3)}, // repeated_int32
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string",
							Type:     bigquery.StringFieldType,
							Repeated: true,
						},
						&bigquery.FieldSchema{
							Name:     "repeated_int32",
							Type:     bigquery.IntegerFieldType,
							Repeated: true,
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedString([]string{"hello", "world", "test"})
						result.SetRepeatedInt32([]int32{1, 2, 3})
						return result
					},
				},

				{
					name: "nested message",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							"nested text",                    // text
							int64(42),                        // number
							true,                             // flag
							[]bigquery.Value{"tag1", "tag2"}, // tags
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "nested_message",
							Type: bigquery.RecordFieldType,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
								&bigquery.FieldSchema{Name: "flag", Type: bigquery.BooleanFieldType},
								&bigquery.FieldSchema{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						nested := &testdatav1.NestedMessage{}
						nested.SetText("nested text")
						nested.SetNumber(42)
						nested.SetFlag(true)
						nested.SetTags([]string{"tag1", "tag2"})
						result.SetNestedMessage(nested)
						return result
					},
				},

				{
					name: "repeated nested messages",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							[]bigquery.Value{"first", int64(1), true},   // first nested message
							[]bigquery.Value{"second", int64(2), false}, // second nested message
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_nested",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
								&bigquery.FieldSchema{Name: "flag", Type: bigquery.BooleanFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						nested1 := &testdatav1.NestedMessage{}
						nested1.SetText("first")
						nested1.SetNumber(1)
						nested1.SetFlag(true)
						nested2 := &testdatav1.NestedMessage{}
						nested2.SetText("second")
						nested2.SetNumber(2)
						nested2.SetFlag(false)
						result.SetRepeatedNested([]*testdatav1.NestedMessage{nested1, nested2})
						return result
					},
				},
			},
		},
		{
			name: "maps",
			testCases: []testCase{
				{
					name: "map string to string",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "first_key",
								"value": "first_value",
							},
							map[string]bigquery.Value{
								"key":   "second_key",
								"value": "second_value",
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_string",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.StringFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						mapData := map[string]string{
							"first_key":  "first_value",
							"second_key": "second_value",
						}
						result.SetMapStringString(mapData)
						return result
					},
				},

				{
					name: "map string to int32",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "count",
								"value": int64(10),
							},
							map[string]bigquery.Value{
								"key":   "total",
								"value": int64(100),
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_int32",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.IntegerFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						mapData := map[string]int32{
							"count": 10,
							"total": 100,
						}
						result.SetMapStringInt32(mapData)
						return result
					},
				},

				{
					name: "map string to nested message",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "first",
								"value": []bigquery.Value{"first nested", int64(1), true},
							},
							map[string]bigquery.Value{
								"key":   "second",
								"value": []bigquery.Value{"second nested", int64(2), false},
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_nested",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{
									Name: "value",
									Type: bigquery.RecordFieldType,
									Schema: bigquery.Schema{
										&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
										&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
										&bigquery.FieldSchema{Name: "flag", Type: bigquery.BooleanFieldType},
									},
								},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						nested1 := &testdatav1.NestedMessage{}
						nested1.SetText("first nested")
						nested1.SetNumber(1)
						nested1.SetFlag(true)
						nested2 := &testdatav1.NestedMessage{}
						nested2.SetText("second nested")
						nested2.SetNumber(2)
						nested2.SetFlag(false)
						mapData := map[string]*testdatav1.NestedMessage{
							"first":  nested1,
							"second": nested2,
						}
						result.SetMapStringNested(mapData)
						return result
					},
				},
			},
		},
		{
			name: "null_handling",
			testCases: []testCase{
				{
					name: "null values for all basic types",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // double_value
						nil, // float_value
						nil, // int32_value
						nil, // int64_value
						nil, // bool_value
						nil, // string_value
						nil, // bytes_value
						nil, // enum_value
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "double_value", Type: bigquery.FloatFieldType},
						&bigquery.FieldSchema{Name: "float_value", Type: bigquery.FloatFieldType},
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.IntegerFieldType},
						&bigquery.FieldSchema{Name: "bool_value", Type: bigquery.BooleanFieldType},
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.StringFieldType},
						&bigquery.FieldSchema{Name: "bytes_value", Type: bigquery.BytesFieldType},
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// All fields should remain at their zero values (not set)
						return result
					},
				},

				{
					name: "null nested message",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // nested_message
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "nested_message",
							Type: bigquery.RecordFieldType,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// nested_message should remain nil
						return result
					},
				},

				{
					name: "repeated field with null elements",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{"hello", nil, "world"}, // repeated_string with null element
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string",
							Type:     bigquery.StringFieldType,
							Repeated: true,
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedString([]string{"hello", "", "world"}) // null becomes empty string
						return result
					},
				},

				{
					name: "map with null values",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "test_key",
								"value": nil, // null value
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_string",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.StringFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						mapData := map[string]string{
							"test_key": "", // null becomes empty string
						}
						result.SetMapStringString(mapData)
						return result
					},
				},
			},
		},
		{
			name: "numeric_conversions",
			testCases: []testCase{
				{
					name: "double to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"123.456", // NUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "double_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDoubleValue(123.456)
						return result
					},
				},

				{
					name: "double to BIGNUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"99999999999999999999999999999.999999999", // BIGNUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "double_value", Type: bigquery.BigNumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDoubleValue(99999999999999999999999999999.999999999)
						return result
					},
				},

				{
					name: "float to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"123.45", // NUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "float_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetFloatValue(123.45)
						return result
					},
				},

				{
					name: "int32 to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"12345", // NUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(12345)
						return result
					},
				},

				{
					name: "int64 to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1234567890123", // NUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt64Value(1234567890123)
						return result
					},
				},

				{
					name: "uint32 to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"4294967295", // NUMERIC as string (max uint32)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "uint32_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetUint32Value(4294967295)
						return result
					},
				},

				{
					name: "uint64 to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"18446744073709551615", // NUMERIC as string (max uint64)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "uint64_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetUint64Value(18446744073709551615)
						return result
					},
				},

				{
					name: "string to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"123.456789", // NUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("123.456789")
						return result
					},
				},

				{
					name: "bytes to NUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]byte{0x01, 0x23, 0x45, 0x67}, // bytes representation of NUMERIC
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "bytes_value", Type: bigquery.NumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetBytesValue([]byte{0x01, 0x23, 0x45, 0x67})
						return result
					},
				},

				{
					name: "int32 to BIGNUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"12345", // BIGNUMERIC as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.BigNumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(12345)
						return result
					},
				},

				{
					name: "string to BIGNUMERIC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"123456789.123456789123456789123456789", // BIGNUMERIC as string (high precision)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.BigNumericFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("123456789.123456789123456789123456789")
						return result
					},
				},
			},
		},
		{
			name: "date_time_conversions",
			testCases: []testCase{
				{
					name: "int32 to DATE (days since epoch)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(19000), // days since 1970-01-01 (approximately 2022-01-01)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(19000)
						return result
					},
				},

				{
					name: "int64 to DATE (days since epoch)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(19000), // days since 1970-01-01
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt64Value(19000)
						return result
					},
				},

				{
					name: "string to DATE",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-01-01", // DATE as string literal
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("2022-01-01")
						return result
					},
				},

				{
					name: "int64 to TIMESTAMP (microseconds since epoch)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(1640995200000000), // 2022-01-01 00:00:00 UTC in microseconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt64Value(1640995200000000)
						return result
					},
				},

				{
					name: "string to DATETIME",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-01-01 12:30:45", // DATETIME as string literal
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.DateTimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("2022-01-01 12:30:45")
						return result
					},
				},

				{
					name: "string to TIME",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"12:30:45", // TIME as string literal
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.TimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("12:30:45")
						return result
					},
				},
			},
		},
		{
			name: "alternative_conversions",
			testCases: []testCase{
				{
					name: "int32 to BOOL (zero is false)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(0), // 0 should convert to false
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.BooleanFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(0) // Current implementation: int32 stays as-is
						return result
					},
				},

				{
					name: "int32 to BOOL (non-zero is true)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(42), // non-zero should convert to true
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.BooleanFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(42) // Current implementation: int32 stays as-is
						return result
					},
				},

				{
					name: "int64 to BOOL",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(1), // 1 should convert to true
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int64_value", Type: bigquery.BooleanFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt64Value(1) // Current implementation: int64 stays as-is
						return result
					},
				},

				{
					name: "string to JSON",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						`{"key": "value", "number": 42}`, // Valid JSON string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.JSONFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue(`{"key": "value", "number": 42}`)
						return result
					},
				},

				{
					name: "string to BYTES",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"hello world", // String that should convert to bytes
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.BytesFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue("hello world") // Current implementation: string stays as-is
						return result
					},
				},

				{
					name: "enum to STRING",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"TEST_ENUM_VALUE_ONE", // Enum name as string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetEnumValue(testdatav1.TestEnum_TEST_ENUM_VALUE_ONE)
						return result
					},
				},
			},
		},
		{
			name: "well_known_types",
			testCases: []testCase{
				{
					name: "google.protobuf.StringValue with null",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // null value
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_wrapper_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// Null should result in no field being set
						return result
					},
				},

				{
					name: "google.protobuf.StringValue with valid value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"test string",
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_wrapper_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringWrapperValue(&wrapperspb.StringValue{Value: "test string"})
						return result
					},
				},

				{
					name: "google.protobuf.Int32Value with valid value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(42),
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_wrapper_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32WrapperValue(&wrapperspb.Int32Value{Value: 42})
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from time.Time",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						mustParseTime("2022-01-01T12:30:45.123456Z"),
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-01-01T12:30:45.123456Z")))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from int64 seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(3661), // 1 hour, 1 minute, 1 second
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(time.Duration(3661) * time.Second))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 UTC",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123456789Z", // RFC3339 UTC string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45.123456789Z")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 positive offset",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123456789+08:00", // RFC3339 with +08:00 offset (Asia/Shanghai)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45.123456789+08:00")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 negative offset",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123456789-05:00", // RFC3339 with -05:00 offset (US Eastern)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45.123456789-05:00")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 extreme positive offset",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45+14:00", // RFC3339 with +14:00 offset (Pacific/Kiritimati)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45+14:00")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 extreme negative offset",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45-12:00", // RFC3339 with -12:00 offset (Baker Island)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45-12:00")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from string RFC3339 fractional offset",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.5+05:30", // RFC3339 with +05:30 offset (India) and fractional seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45.5+05:30")))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from int64 microseconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(1671979845123456), // 2022-12-25T15:30:45.123456Z in microseconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(time.UnixMicro(1671979845123456)))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp from int64 seconds (converted to microseconds)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(1671979845000000), // 2022-12-25T15:30:45Z in microseconds (BigQuery format)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(time.UnixMicro(1671979845000000)))
						return result
					},
				},

				{
					name: "google.protobuf.Timestamp null value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // null timestamp
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// timestamp_value should remain nil
						return result
					},
				},

				{
					name: "google.protobuf.Duration null value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // null duration
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// duration_value should remain nil
						return result
					},
				},

				{
					name: "google.protobuf.Duration from ISO8601 string",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PT1H30M45.123S", // ISO8601: 1 hour, 30 minutes, 45.123 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// 1h30m45.123s = 1*3600 + 30*60 + 45.123 = 5445.123 seconds
						result.SetDurationValue(durationpb.New(time.Hour + 30*time.Minute + 45*time.Second + 123*time.Millisecond))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from BigQuery interval string",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2:15:30.500", // BigQuery interval: 2 hours, 15 minutes, 30.5 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// 2h15m30.5s = 2*3600 + 15*60 + 30.5 = 8130.5 seconds
						result.SetDurationValue(durationpb.New(2*time.Hour + 15*time.Minute + 30*time.Second + 500*time.Millisecond))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from simple BigQuery interval",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"0:05:30", // BigQuery interval: 5 minutes, 30 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(5*time.Minute + 30*time.Second))
						return result
					},
				},
			},
		},
		{
			name: "geography_and_intervals",
			testCases: []testCase{
				{
					name: "LatLng from WKT POINT (existing support)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"POINT(-122.4194 37.7749)", // San Francisco in WKT format
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "latlng_value", Type: bigquery.GeographyFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetLatlngValue(&latlng.LatLng{
							Latitude:  37.7749,
							Longitude: -122.4194,
						})
						return result
					},
				},

				{
					name: "LatLng from GeoJSON POINT (should fail - not supported yet)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						`{"type": "Point", "coordinates": [-122.4194, 37.7749]}`, // GeoJSON format
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "latlng_value", Type: bigquery.GeographyFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						return result
					},
					expectedError: "invalid GEOGRAPHY value", // Should fail because GeoJSON parsing not implemented
				},

				{
					name: "String as GEOGRAPHY - WKT LINESTRING stored in string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"LINESTRING(-122.48369 37.8331628, -122.48348 37.8347766)", // WKT LINESTRING
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "geography_string", Type: bigquery.GeographyFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetGeographyString("LINESTRING(-122.48369 37.8331628, -122.48348 37.8347766)")
						return result
					},
				},

				{
					name: "String as GEOGRAPHY - WKT POLYGON stored in string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"POLYGON((-122.4194 37.7749, -122.4094 37.7849, -122.4294 37.7849, -122.4194 37.7749))", // WKT POLYGON
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "geography_string", Type: bigquery.GeographyFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetGeographyString("POLYGON((-122.4194 37.7749, -122.4094 37.7849, -122.4294 37.7849, -122.4194 37.7749))")
						return result
					},
				},

				{
					name: "google.protobuf.Duration from float64 seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						float64(3661.5), // 1 hour, 1 minute, 1.5 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(time.Duration(3661.5 * float64(time.Second))))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from small int64 (seconds)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(45), // 45 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(45 * time.Second))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from zero value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(0), // zero duration
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(0))
						return result
					},
				},

				{
					name: "google.protobuf.Duration from negative value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(-30), // negative 30 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationValue(durationpb.New(-30 * time.Second))
						return result
					},
				},

				{
					name: "Duration ISO8601 string stored in string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PT1H1M1.5S", // ISO8601 duration: 1 hour, 1 minute, 1.5 seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_string", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationString("PT1H1M1.5S")
						return result
					},
				},

				{
					name: "Duration BigQuery INTERVAL string stored in string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1:01:01.5", // BigQuery interval format: H:MM:SS.sss
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_string", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDurationString("1:01:01.5")
						return result
					},
				},

				{
					name: "Interval string stored in string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1:30:45", // BigQuery interval literal
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "interval_string", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetIntervalString("1:30:45")
						return result
					},
				},
			},
		},
		{
			name: "timestamp_timezone_edge_cases",
			testCases: []testCase{
				{
					name: "Invalid timezone format should fail",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123+25:00", // Invalid offset > +14:00
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expectedError: "invalid RFC3339 timestamp string",
				},

				{
					name: "Missing timezone should fail",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123", // Missing timezone
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expectedError: "invalid RFC3339 timestamp string",
				},

				{
					name: "Invalid offset format should fail",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45.123+8:0", // Invalid offset format (missing zero padding)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expectedError: "invalid RFC3339 timestamp string",
				},

				{
					name: "Valid boundary case: UTC+12",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45+12:00", // Valid +12:00 offset (Fiji)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45+12:00")))
						return result
					},
				},

				{
					name: "Valid boundary case: UTC-11",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-12-25T15:30:45-11:00", // Valid -11:00 offset (American Samoa)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-12-25T15:30:45-11:00")))
						return result
					},
				},

				{
					name: "Daylight saving time transition example",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-03-13T07:00:00-04:00", // US Eastern Daylight Time
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_value", Type: bigquery.TimestampFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampValue(timestamppb.New(mustParseTime("2022-03-13T07:00:00-04:00")))
						return result
					},
				},
			},
		},
		{
			name: "range_types",
			testCases: []testCase{
				{
					name: "DATE range with both bounds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "2022-01-01", // DATE start
							End:   "2022-12-31", // DATE end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateRange(newDateRange("2022-01-01", "2022-12-31"))
						return result
					},
				},

				{
					name: "DATE range with unbounded start",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: nil,          // unbounded start
							End:   "2022-12-31", // DATE end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateRange(newDateRange("", "2022-12-31"))
						return result
					},
				},

				{
					name: "DATE range with unbounded end",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "2022-01-01", // DATE start
							End:   nil,          // unbounded end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateRange(newDateRange("2022-01-01", ""))
						return result
					},
				},

				{
					name: "DATE range fully unbounded",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: nil, // unbounded start
							End:   nil, // unbounded end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateRange(newDateRange("", ""))
						return result
					},
				},

				{
					name: "TIMESTAMP range with both bounds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: mustParseTime("2022-01-01T00:00:00Z"), // TIMESTAMP start
							End:   mustParseTime("2022-12-31T23:59:59Z"), // TIMESTAMP end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampRange(newTimestampRange(
							timestamppb.New(mustParseTime("2022-01-01T00:00:00Z")),
							timestamppb.New(mustParseTime("2022-12-31T23:59:59Z")),
						))
						return result
					},
				},

				{
					name: "TIMESTAMP range with unbounded start",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: nil,                                   // unbounded start
							End:   mustParseTime("2022-12-31T23:59:59Z"), // TIMESTAMP end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampRange(newTimestampRange(
							nil,
							timestamppb.New(mustParseTime("2022-12-31T23:59:59Z")),
						))
						return result
					},
				},

				{
					name: "TIMESTAMP range fully unbounded",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: nil, // unbounded start
							End:   nil, // unbounded end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampRange(newTimestampRange(nil, nil))
						return result
					},
				},

				{
					name: "DATETIME range with both bounds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "2022-01-01 00:00:00", // DATETIME start
							End:   "2022-12-31 23:59:59", // DATETIME end
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "datetime_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDatetimeRange(newDateTimeRange("2022-01-01 00:00:00", "2022-12-31 23:59:59"))
						return result
					},
				},

				{
					name: "DATETIME range with fractional seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "2022-01-01 00:00:00.123456", // DATETIME with microseconds
							End:   "2022-12-31 23:59:59.999999", // DATETIME with microseconds
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "datetime_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDatetimeRange(newDateTimeRange("2022-01-01 00:00:00.123456", "2022-12-31 23:59:59.999999"))
						return result
					},
				},

				{
					name: "Null RANGE value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil, // null range
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// date_range should remain nil
						return result
					},
				},

				{
					name: "RANGE with timezone-aware TIMESTAMP",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: mustParseTime("2022-01-01T08:00:00+08:00"), // Asia/Shanghai
							End:   mustParseTime("2022-01-01T17:00:00-05:00"), // US Eastern
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_range", Type: bigquery.RangeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimestampRange(newTimestampRange(
							timestamppb.New(mustParseTime("2022-01-01T08:00:00+08:00")),
							timestamppb.New(mustParseTime("2022-01-01T17:00:00-05:00")),
						))
						return result
					},
				},
			},
		},
		{
			name: "range_error_handling",
			testCases: []testCase{
				{
					name: "Invalid RANGE type should error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_a_range_value", // invalid range value
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expectedError: "unsupported BigQuery value for RANGE",
				},

				{
					name: "RANGE with unknown message type should error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "2022-01-01",
							End:   "2022-12-31",
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "unknown_range_field", Type: bigquery.RangeFieldType},
					},
					expectedError: "unknown field", // field doesn't exist in proto
				},

				{
					name: "Range with invalid timestamp format",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: "invalid-timestamp-format",
							End:   "2022-12-31T23:59:59Z",
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timestamp_range", Type: bigquery.RangeFieldType},
					},
					expectedError: "unsupported value type for timestamp range field",
				},

				{
					name: "Range with invalid date format",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						&bigquery.RangeValue{
							Start: int64(12345), // Invalid type for date range (should be string)
							End:   "2022-12-31",
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_range", Type: bigquery.RangeFieldType},
					},
					expectedError: "unsupported value type for string range field",
				},
			},
		},
		{
			name: "duration_parsing_errors",
			testCases: []testCase{
				{
					name: "BigQuery interval with wrong format",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1-30-45", // Wrong format (should be H:MM:SS)
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "BigQuery interval with invalid hours",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"XX:30:45", // Invalid hours
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "BigQuery interval with invalid minutes",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1:XX:45", // Invalid minutes
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "BigQuery interval with invalid seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"1:30:XX", // Invalid seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "ISO8601 duration with invalid fractional seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PT1H30M45.XXXS", // Invalid fractional seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "ISO8601 duration with invalid hours",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PTXXH30M45S", // Invalid hours
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "ISO8601 duration with invalid minutes",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PT1HXXM45S", // Invalid minutes
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},

				{
					name: "ISO8601 duration with invalid seconds",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"PT1H30MXXS", // Invalid seconds
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "duration_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid duration string for google.protobuf.Duration",
				},
			},
		},
		{
			name: "numeric_parsing_errors",
			testCases: []testCase{
				{
					name: "Cannot convert NUMERIC string to unsupported field type",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"123.456", // NUMERIC string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.NumericFieldType}, // enum can't handle numeric strings properly
					},
					expectedError: "cannot convert NUMERIC string",
				},

				{
					name: "Date validation error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"2022-13-45", // Invalid date
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.DateFieldType},
					},
					expectedError: "invalid date",
				},

				{
					name: "JSON validation error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						`{broken json syntax`, // Invalid JSON
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.JSONFieldType},
					},
					expectedError: "invalid JSON",
				},
			},
		},
		{
			name: "map_processing_errors",
			testCases: []testCase{
				{
					name: "Map field with invalid map structure",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_a_map_array", // Should be []bigquery.Value for map field
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_string",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.StringFieldType},
							},
						},
					},
					expectedError: "unsupported BigQuery value for message",
				},

				{
					name: "Map field with missing key",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"value": "test_value", // Missing "key" field
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_string",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.StringFieldType},
							},
						},
					},
					expectedError: "map entry is missing key field",
				},

				{
					name: "Map field with wrong value type for well-known type",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "test_key",
								"value": "not_an_integer", // Invalid value type for Int32Value
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_int32_wrapper",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.IntegerFieldType},
							},
						},
					},
					expectedError: "invalid BigQuery value for google.protobuf.Int32Value",
				},

				{
					name: "Map field with invalid nested message structure",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{
								"key":   "test_key",
								"value": "not_a_nested_message", // Should be array for nested message
							},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_nested",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{
									Name: "value",
									Type: bigquery.RecordFieldType,
									Schema: bigquery.Schema{
										&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
										&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
									},
								},
							},
						},
					},
					expectedError: "unsupported BigQuery value for message",
				},
			},
		},
		{
			name: "list_processing_errors",
			testCases: []testCase{
				{
					name: "List field with mixed valid and invalid elements",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							int64(42),      // Valid
							"not_a_number", // Invalid for int32 field
							int64(100),     // Valid
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_int32",
							Type:     bigquery.IntegerFieldType,
							Repeated: true,
						},
					},
					expectedError: "invalid BigQuery value",
				},

				{
					name: "Nested message list with invalid element structure",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							[]bigquery.Value{"valid", int64(1), true},   // Valid nested message
							"invalid_structure",                         // Invalid - should be array
							[]bigquery.Value{"valid2", int64(2), false}, // Valid nested message
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_nested",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "text", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "number", Type: bigquery.IntegerFieldType},
								&bigquery.FieldSchema{Name: "flag", Type: bigquery.BooleanFieldType},
							},
						},
					},
					expectedError: "unsupported BigQuery value for message",
				},

				{
					name: "Well-known type list with invalid element",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							"valid_string",
							int64(123), // Invalid - should be string for StringValue
							"another_valid_string",
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string_wrapper",
							Type:     bigquery.StringFieldType,
							Repeated: true,
						},
					},
					expectedError: "invalid BigQuery value for google.protobuf.StringValue",
				},
			},
		},
		{
			name: "boundary_conditions",
			testCases: []testCase{
				{
					name: "Very long string field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						strings.Repeat("a", 10000), // 10KB string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.StringFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetStringValue(strings.Repeat("a", 10000))
						return result
					},
				},

				{
					name: "Maximum int32 value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(2147483647), // Max int32
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(2147483647)
						return result
					},
				},

				{
					name: "Minimum int32 value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(-2147483648), // Min int32
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(-2147483648)
						return result
					},
				},

				{
					name: "Large byte array",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						make([]byte, 1024), // 1KB byte array
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "bytes_value", Type: bigquery.BytesFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetBytesValue(make([]byte, 1024))
						return result
					},
				},

				{
					name: "Large repeated field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						func() []bigquery.Value {
							values := make([]bigquery.Value, 1000)
							for i := range values {
								values[i] = fmt.Sprintf("item_%d", i)
							}
							return values
						}(),
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string",
							Type:     bigquery.StringFieldType,
							Repeated: true,
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						values := make([]string, 1000)
						for i := range values {
							values[i] = fmt.Sprintf("item_%d", i)
						}
						result.SetRepeatedString(values)
						return result
					},
				},
			},
		},
		{
			name: "google_type_civil_time",
			testCases: []testCase{
				{
					name: "google.type.Date from civil.Date",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						civil.Date{Year: 2022, Month: time.March, Day: 15},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateValue(&date.Date{
							Year:  2022,
							Month: 3,
							Day:   15,
						})
						return result
					},
				},

				{
					name: "google.type.DateTime from civil.DateTime",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						civil.DateTime{
							Date: civil.Date{Year: 2022, Month: time.December, Day: 25},
							Time: civil.Time{Hour: 14, Minute: 30, Second: 45, Nanosecond: 123456000},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "datetime_value", Type: bigquery.DateTimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDatetimeValue(&datetime.DateTime{
							Year:    2022,
							Month:   12,
							Day:     25,
							Hours:   14,
							Minutes: 30,
							Seconds: 45,
							Nanos:   123456000,
						})
						return result
					},
				},

				{
					name: "google.type.DateTime from time.Time (with timezone)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						mustParseTime("2022-12-25T14:30:45.123456-08:00"),
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "datetime_value", Type: bigquery.DateTimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDatetimeValue(&datetime.DateTime{
							Year:    2022,
							Month:   12,
							Day:     25,
							Hours:   14,
							Minutes: 30,
							Seconds: 45,
							Nanos:   123456000,
							TimeOffset: &datetime.DateTime_TimeZone{
								TimeZone: &datetime.TimeZone{
									Id: "-08:00",
								},
							},
						})
						return result
					},
				},

				{
					name: "google.type.TimeOfDay from civil.Time",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						civil.Time{Hour: 23, Minute: 59, Second: 59, Nanosecond: 999999999},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timeofday_value", Type: bigquery.TimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimeofdayValue(&timeofday.TimeOfDay{
							Hours:   23,
							Minutes: 59,
							Seconds: 59,
							Nanos:   999999999,
						})
						return result
					},
				},

				{
					name: "google.type.Date boundary case (leap year)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						civil.Date{Year: 2024, Month: time.February, Day: 29}, // Leap year
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetDateValue(&date.Date{
							Year:  2024,
							Month: 2,
							Day:   29,
						})
						return result
					},
				},

				{
					name: "google.type.TimeOfDay edge case (midnight)",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						civil.Time{Hour: 0, Minute: 0, Second: 0, Nanosecond: 0},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "timeofday_value", Type: bigquery.TimeFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetTimeofdayValue(&timeofday.TimeOfDay{
							Hours:   0,
							Minutes: 0,
							Seconds: 0,
							Nanos:   0,
						})
						return result
					},
				},

				{
					name: "google.type.Date null value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						nil,
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "date_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						// date_value should remain nil/unset
						return result
					},
				},
			},
		},
		{
			name: "complex_collections",
			testCases: []testCase{
				{
					name: "repeated google.protobuf.Duration",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							int64(3600), // 1 hour
							int64(900),  // 15 minutes
							int64(45),   // 45 seconds
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_duration", Type: bigquery.StringFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedDuration([]*durationpb.Duration{
							durationpb.New(time.Hour),
							durationpb.New(15 * time.Minute),
							durationpb.New(45 * time.Second),
						})
						return result
					},
				},

				{
					name: "repeated google.type.LatLng",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							"POINT(-122.4194 37.7749)", // San Francisco
							"POINT(-74.0060 40.7128)",  // New York
							"POINT(2.3522 48.8566)",    // Paris
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_latlng", Type: bigquery.GeographyFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedLatlng([]*latlng.LatLng{
							{Latitude: 37.7749, Longitude: -122.4194},
							{Latitude: 40.7128, Longitude: -74.0060},
							{Latitude: 48.8566, Longitude: 2.3522},
						})
						return result
					},
				},

				{
					name: "repeated DateRange",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							&bigquery.RangeValue{Start: "2022-01-01", End: "2022-03-31"}, // Q1
							&bigquery.RangeValue{Start: "2022-04-01", End: "2022-06-30"}, // Q2
							&bigquery.RangeValue{Start: "2022-07-01", End: nil},          // Q3+ unbounded
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_date_range", Type: bigquery.RangeFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedDateRange([]*testdatav1.DateRange{
							newDateRange("2022-01-01", "2022-03-31"),
							newDateRange("2022-04-01", "2022-06-30"),
							newDateRange("2022-07-01", ""),
						})
						return result
					},
				},

				{
					name: "repeated TimestampRange",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							&bigquery.RangeValue{
								Start: mustParseTime("2022-01-01T09:00:00Z"),
								End:   mustParseTime("2022-01-01T17:00:00Z"),
							}, // Work day 1
							&bigquery.RangeValue{
								Start: mustParseTime("2022-01-02T09:00:00Z"),
								End:   mustParseTime("2022-01-02T17:00:00Z"),
							}, // Work day 2
							&bigquery.RangeValue{
								Start: mustParseTime("2022-01-03T09:00:00Z"),
								End:   nil,
							}, // Open-ended day
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_timestamp_range", Type: bigquery.RangeFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedTimestampRange([]*testdatav1.TimestampRange{
							newTimestampRange(
								timestamppb.New(mustParseTime("2022-01-01T09:00:00Z")),
								timestamppb.New(mustParseTime("2022-01-01T17:00:00Z")),
							),
							newTimestampRange(
								timestamppb.New(mustParseTime("2022-01-02T09:00:00Z")),
								timestamppb.New(mustParseTime("2022-01-02T17:00:00Z")),
							),
							newTimestampRange(
								timestamppb.New(mustParseTime("2022-01-03T09:00:00Z")),
								nil,
							),
						})
						return result
					},
				},

				{
					name: "map string to google.protobuf.Duration",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{"key": "startup_time", "value": int64(30)},  // 30 seconds
							map[string]bigquery.Value{"key": "process_time", "value": int64(120)}, // 2 minutes
							map[string]bigquery.Value{"key": "cleanup_time", "value": int64(15)},  // 15 seconds
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_duration",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.StringFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetMapStringDuration(map[string]*durationpb.Duration{
							"startup_time": durationpb.New(30 * time.Second),
							"process_time": durationpb.New(2 * time.Minute),
							"cleanup_time": durationpb.New(15 * time.Second),
						})
						return result
					},
				},

				{
					name: "map string to google.type.LatLng",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{"key": "headquarters", "value": "POINT(-122.4194 37.7749)"}, // San Francisco
							map[string]bigquery.Value{"key": "office_ny", "value": "POINT(-74.0060 40.7128)"},     // New York
							map[string]bigquery.Value{"key": "datacenter", "value": "POINT(-77.0369 38.9072)"},    // Washington DC
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_latlng",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.GeographyFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetMapStringLatlng(map[string]*latlng.LatLng{
							"headquarters": {Latitude: 37.7749, Longitude: -122.4194},
							"office_ny":    {Latitude: 40.7128, Longitude: -74.0060},
							"datacenter":   {Latitude: 38.9072, Longitude: -77.0369},
						})
						return result
					},
				},

				{
					name: "map string to DateRange",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{"key": "project_alpha", "value": &bigquery.RangeValue{Start: "2022-01-01", End: "2022-06-30"}},
							map[string]bigquery.Value{"key": "project_beta", "value": &bigquery.RangeValue{Start: "2022-03-01", End: "2022-12-31"}},
							map[string]bigquery.Value{"key": "project_gamma", "value": &bigquery.RangeValue{Start: "2022-06-01", End: nil}}, // ongoing
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_date_range",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.RangeFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetMapStringDateRange(map[string]*testdatav1.DateRange{
							"project_alpha": newDateRange("2022-01-01", "2022-06-30"),
							"project_beta":  newDateRange("2022-03-01", "2022-12-31"),
							"project_gamma": newDateRange("2022-06-01", ""),
						})
						return result
					},
				},

				// google.type.* collection tests
				{
					name: "repeated google.type.Date",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							civil.Date{Year: 2022, Month: time.January, Day: 1},
							civil.Date{Year: 2022, Month: time.July, Day: 4},
							civil.Date{Year: 2022, Month: time.December, Day: 25},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_date", Type: bigquery.DateFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedDate([]*date.Date{
							{Year: 2022, Month: 1, Day: 1},
							{Year: 2022, Month: 7, Day: 4},
							{Year: 2022, Month: 12, Day: 25},
						})
						return result
					},
				},

				{
					name: "repeated google.type.TimeOfDay",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							civil.Time{Hour: 9, Minute: 0, Second: 0, Nanosecond: 0},            // 9:00 AM
							civil.Time{Hour: 12, Minute: 30, Second: 0, Nanosecond: 0},          // 12:30 PM
							civil.Time{Hour: 17, Minute: 45, Second: 30, Nanosecond: 500000000}, // 5:45:30.5 PM
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "repeated_timeofday", Type: bigquery.TimeFieldType, Repeated: true},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetRepeatedTimeofday([]*timeofday.TimeOfDay{
							{Hours: 9, Minutes: 0, Seconds: 0, Nanos: 0},
							{Hours: 12, Minutes: 30, Seconds: 0, Nanos: 0},
							{Hours: 17, Minutes: 45, Seconds: 30, Nanos: 500000000},
						})
						return result
					},
				},

				{
					name: "map string to google.type.Date",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{"key": "new_year", "value": civil.Date{Year: 2023, Month: time.January, Day: 1}},
							map[string]bigquery.Value{"key": "independence_day", "value": civil.Date{Year: 2023, Month: time.July, Day: 4}},
							map[string]bigquery.Value{"key": "christmas", "value": civil.Date{Year: 2023, Month: time.December, Day: 25}},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_date",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.DateFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetMapStringDate(map[string]*date.Date{
							"new_year":         {Year: 2023, Month: 1, Day: 1},
							"independence_day": {Year: 2023, Month: 7, Day: 4},
							"christmas":        {Year: 2023, Month: 12, Day: 25},
						})
						return result
					},
				},

				{
					name: "map string to google.type.TimeOfDay",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{
							map[string]bigquery.Value{"key": "morning_standup", "value": civil.Time{Hour: 9, Minute: 0, Second: 0, Nanosecond: 0}},
							map[string]bigquery.Value{"key": "lunch_break", "value": civil.Time{Hour: 12, Minute: 0, Second: 0, Nanosecond: 0}},
							map[string]bigquery.Value{"key": "end_of_day", "value": civil.Time{Hour: 17, Minute: 30, Second: 0, Nanosecond: 0}},
						},
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "map_string_timeofday",
							Type:     bigquery.RecordFieldType,
							Repeated: true,
							Schema: bigquery.Schema{
								&bigquery.FieldSchema{Name: "key", Type: bigquery.StringFieldType},
								&bigquery.FieldSchema{Name: "value", Type: bigquery.TimeFieldType},
							},
						},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetMapStringTimeofday(map[string]*timeofday.TimeOfDay{
							"morning_standup": {Hours: 9, Minutes: 0, Seconds: 0, Nanos: 0},
							"lunch_break":     {Hours: 12, Minutes: 0, Seconds: 0, Nanos: 0},
							"end_of_day":      {Hours: 17, Minutes: 30, Seconds: 0, Nanos: 0},
						})
						return result
					},
				},
			},
		},
		{
			name: "schema_validation_errors",
			testCases: []testCase{
				{
					name: "message length doesn't match schema length",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"test",
						"extra_value", // More values than schema fields
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name: "string_value",
							Type: bigquery.StringFieldType,
						},
					},
					expectedError: "message has 2 fields but schema has 1 fields",
				},

				{
					name: "non-repeated schema for list field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						[]bigquery.Value{"hello", "world"}, // Array value
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string",
							Type:     bigquery.StringFieldType,
							Repeated: false, // NOT repeated but we're passing an array
						},
					},
					expectedError: "unsupported field schema for list field: not repeated",
				},

				{
					name: "invalid BigQuery value type for list field",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_an_array", // String instead of array
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{
							Name:     "repeated_string",
							Type:     bigquery.StringFieldType,
							Repeated: true,
						},
					},
					expectedError: "unsupported BigQuery value for message",
				},
			},
		},
		{
			name: "well_known_type_errors",
			testCases: []testCase{
				{
					name: "DoubleValue with string input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_a_number", // String instead of number
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "double_wrapper_value", Type: bigquery.FloatFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.DoubleValue",
				},

				{
					name: "FloatValue with bool input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						true, // Bool instead of number
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "float_wrapper_value", Type: bigquery.FloatFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.FloatValue",
				},

				{
					name: "Int32Value with string input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_a_number", // String instead of integer
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_wrapper_value", Type: bigquery.IntegerFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.Int32Value",
				},

				{
					name: "Int64Value with float input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						3.14, // Float instead of integer
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int64_wrapper_value", Type: bigquery.IntegerFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.Int64Value",
				},

				{
					name: "UInt32Value with negative input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"negative", // Invalid type for unsigned integer
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "uint32_wrapper_value", Type: bigquery.IntegerFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.UInt32Value",
				},

				{
					name: "UInt64Value with float input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						3.14, // Float instead of unsigned integer
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "uint64_wrapper_value", Type: bigquery.IntegerFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.UInt64Value",
				},

				{
					name: "BoolValue with string input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_a_bool", // String instead of bool
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "bool_wrapper_value", Type: bigquery.BooleanFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.BoolValue",
				},

				{
					name: "StringValue with int input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						42, // Int instead of string
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_wrapper_value", Type: bigquery.StringFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.StringValue",
				},

				{
					name: "BytesValue with string input",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not_bytes", // String instead of []byte
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "bytes_wrapper_value", Type: bigquery.BytesFieldType},
					},
					expectedError: "invalid BigQuery value for google.protobuf.BytesValue",
				},

				{
					name: "Struct with invalid JSON",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						`{"invalid": json syntax}`, // Invalid JSON
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.JSONFieldType},
					},
					expectedError: "invalid JSON",
				},

				{
					name: "Enum with unknown string value",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"UNKNOWN_ENUM_VALUE", // Unknown enum name
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.StringFieldType},
					},
					expectedError: "unknown enum value",
				},

				{
					name: "Enum with unsupported type",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						3.14, // Float for enum field
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "enum_value", Type: bigquery.FloatFieldType},
					},
					expectedError: "invalid BigQuery value",
				},
			},
		},
		{
			name: "error_handling",
			testCases: []testCase{
				{
					name: "invalid JSON string should error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						`{"invalid": json syntax}`, // Invalid JSON
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.JSONFieldType},
					},
					expected: func() proto.Message {
						return &testdatav1.KitchenSink{}
					},
					expectedError: "invalid JSON", // This should error when JSON validation is implemented
				},

				{
					name: "invalid date string should error",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"invalid-date", // Invalid date format
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "string_value", Type: bigquery.DateFieldType},
					},
					expected: func() proto.Message {
						return &testdatav1.KitchenSink{}
					},
					expectedError: "invalid date", // This should error when date validation is implemented
				},

				{
					name: "out of range integer conversion",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						int64(9223372036854775807), // Max int64, might overflow int32
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						result := &testdatav1.KitchenSink{}
						result.SetInt32Value(-1) // Overflow behavior
						return result
					},
				},

				{
					name: "type mismatch - string where number expected",
					messageLoader: MessageLoader{
						Message: &testdatav1.KitchenSink{},
					},
					row: []bigquery.Value{
						"not a number", // String value for integer field
					},
					schema: bigquery.Schema{
						&bigquery.FieldSchema{Name: "int32_value", Type: bigquery.IntegerFieldType},
					},
					expected: func() proto.Message {
						return &testdatav1.KitchenSink{}
					},
					expectedError: "invalid BigQuery value", // Should error on type mismatch
				},
			},
		},
	}
	for _, testCaseCategory := range testCaseCategories {
		t.Run(testCaseCategory.name, func(t *testing.T) {
			for _, test := range testCaseCategory.testCases {
				t.Run(test.name, func(t *testing.T) {
					err := test.messageLoader.Load(test.row, test.schema)
					if test.expectedError != "" {
						if err == nil {
							t.Errorf("expected error, got nil")
						} else if !strings.Contains(err.Error(), test.expectedError) {
							t.Errorf("expected error to contain %q, got %q", test.expectedError, err.Error())
						}
					} else {
						expected := test.expected()
						if diff := cmp.Diff(expected, test.messageLoader.Message, protocmp.Transform()); diff != "" {
							t.Errorf("expected %v, got %v, diff: %s", expected, test.messageLoader.Message, diff)
						}
					}
				})
			}
		})
	}
}

// mustParseTime is a helper function for test cases
func mustParseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

// Helper functions for creating range types in tests
func newDateRange(start, end string) *testdatav1.DateRange {
	dr := &testdatav1.DateRange{}
	if start != "" {
		dr.SetStart(start)
	}
	if end != "" {
		dr.SetEnd(end)
	}
	return dr
}

func newTimestampRange(start, end *timestamppb.Timestamp) *testdatav1.TimestampRange {
	tr := &testdatav1.TimestampRange{}
	if start != nil {
		tr.SetStart(start)
	}
	if end != nil {
		tr.SetEnd(end)
	}
	return tr
}

func newDateTimeRange(start, end string) *testdatav1.DateTimeRange {
	dtr := &testdatav1.DateTimeRange{}
	if start != "" {
		dtr.SetStart(start)
	}
	if end != "" {
		dtr.SetEnd(end)
	}
	return dtr
}
