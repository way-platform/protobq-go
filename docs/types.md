# Supported types

This document contains excerpts of type mapping documentation from the BigQuery
and Protobuf Go SDKs, with the purpose of providing a starting point for read
paths that should be supported by this library.

## Write paths

### Pub/Sub BigQuery Subscriptions

See also: https://cloud.google.com/pubsub/docs/bigquery#protocol-buffer-types

| Protobuf Type             | BigQuery Types                                                                        |
| ------------------------- | ------------------------------------------------------------------------------------- |
| `double`                  | `FLOAT64`, `NUMERIC`, or `BIGNUMERIC`                                                 |
| `float`                   | `FLOAT64`, `NUMERIC`, or `BIGNUMERIC`                                                 |
| `int32`                   | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, or `DATE`                                         |
| `int64`                   | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, `DATE`, `DATETIME`, or `TIMESTAMP`                |
| `uint32`                  | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, or `DATE`                                         |
| `uint64`                  | `NUMERIC` or `BIGNUMERIC`                                                             |
| `sint32`                  | `INTEGER`, `NUMERIC`, or `BIGNUMERIC`                                                 |
| `sint64`                  | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, `DATE`, `DATETIME`, or `TIMESTAMP`                |
| `fixed32`                 | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, or `DATE`                                         |
| `fixed64`                 | `NUMERIC` or `BIGNUMERIC`                                                             |
| `sfixed32`                | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, or `DATE`                                         |
| `sfixed64`                | `INTEGER`, `NUMERIC`, `BIGNUMERIC`, `DATE`, `DATETIME`, or `TIMESTAMP`                |
| `bool`                    | `BOOLEAN`                                                                             |
| `string`                  | `STRING`, `JSON`, `TIMESTAMP`, `DATETIME`, `DATE`, `TIME`, `NUMERIC`, or `BIGNUMERIC` |
| `bytes`                   | `BYTES`, `NUMERIC`, or `BIGNUMERIC`                                                   |
| `enum`                    | `INTEGER`                                                                             |
| `message`                 | `RECORD` / `STRUCT`                                                                   |
| `oneof`                   | Unmappable                                                                            |
| `map<KeyType, ValueType>` | `REPEATED RECORD<key KeyType, value ValueType>`                                       |
| `enum`                    | `INTEGER`                                                                             |
| `repeated`                | `REPEATED`                                                                            |

### BigQuery Storage Write API

See also: https://cloud.google.com/bigquery/docs/supported-data-types#supported_protocol_buffer_data_types

| BigQuery data type      | Supported protocol buffer types                                                                                               |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `BOOL`                  | `bool`, `int32`, `int64`, `uint32`, `uint64`, `google.protobuf.BoolValue`                                                     |
| `BYTES`                 | `bytes`, `string`, `google.protobuf.BytesValue`                                                                               |
| `DATE`                  | `int32` (preferred), `int64`, `string` (1)                                                                                    |
| `DATETIME,` `TIME`      | `string` (2) `int64` (3)                                                                                                      |
| `FLOAT`                 | `double`, `float`, `google.protobuf.DoubleValue`, `google.protobuf.FloatValue`                                                |
| `GEOGRAPHY`             | `string` (4)                                                                                                                  |
| `INTEGER`               | `int32`, `int64`, `uint32`, `enum`, `google.protobuf.Int32Value`, `google.protobuf.Int64Value`, `google.protobuf.UInt32Value` |
| `JSON`                  | `string`                                                                                                                      |
| `NUMERIC,` `BIGNUMERIC` | `int32`, `int64`, `uint32`, `uint64`, `double`, `float`, `string`, `bytes`, `google.protobuf.BytesValue` (5)                  |
| `STRING`                | `string`, `enum`, `google.protobuf.StringValue`                                                                               |
| `TIME`                  | `string` (6)                                                                                                                  |
| `TIMESTAMP`             | `int64` (preferred), `int32`, `uint32`, `google.protobuf.Timestamp` (7)                                                       |
| `INTERVAL`              | `string`, `google.protobuf.Duration` (8)                                                                                      |
| `RANGE<T>`              | `message` (9)                                                                                                                 |
| `REPEATED` field        | `repeated` field (10)                                                                                                         |
| `RECORD`                | `message` (11)                                                                                                                |

- (1): The value is the number of days since the Unix epoch (1970-01-01). The
  valid range is -719162 (0001-01-01) to 2932896 (9999-12-31).

- (2): The value must be a DATETIME or TIME literal.

- (3): Use the CivilTimeEncoder class to perform the conversion.

- (4): The value is a geometry in either WKT or GeoJson format.

- (5): Use the BigDecimalByteStringEncoder class to perform the conversion.

- (6): The value must be a TIME literal.

- (7): The value is given in microseconds since the Unix epoch (1970-01-01).

- (8): The string value must be an INTERVAL literal.

- (9): A nested message type in the proto with two fields, start and end, where
  both fields must be of the same supported protocol buffer type that corresponds
  to a BigQuery data type T. T must be one of DATE, DATETIME, or TIMESTAMP. If a
  field (start or end) is not set in the proto message, it represents an unbounded
  boundary. In the following example, f_range_date represents a RANGE column in a
  table. Since the end field is not set in the proto message, the end boundary of
  this range is unbounded.

  ```
  {
    f_range_date: {
      start: 1
    }
  }
  ```

- (10): An array type in the proto corresponds to a repeated field in BigQuery.

- (11): A nested message type in the proto corresponds to a record field in
  BigQuery.

## Go Types

The following is not a complete table of type mappings for BigQuery and Go, just
those handled by
[`bigquery.InferSchema`](https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema).

| BigQuery Type | Go Types                                                              |
| ------------- | --------------------------------------------------------------------- |
| `STRING`      | `string`                                                              |
| `BOOL`        | `bool`                                                                |
| `INTEGER`     | `int`, `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32` |
| `FLOAT`       | `float32`, `float64`                                                  |
| `BYTES`       | `[]byte`                                                              |
| `TIMESTAMP`   | `time.Time`                                                           |
| `DATE`        | `civil.Date`                                                          |
| `TIME`        | `civil.Time`                                                          |
| `DATETIME`    | `civil.DateTime`                                                      |
| `NUMERIC`     | `*big.Rat`                                                            |
| `JSON`        | `map[string]interface{}`                                              |
| `INTERVAL`    | `*bigquery.IntervalValue`                                             |
| `RANGE`       | `*bigquery.RangeValue`                                                |

Nullable fields are inferred from the NullXXX types, declared in the `bigquery` package.

| Nullable BigQuery Type | Go Type                  |
| ---------------------- | ------------------------ |
| `STRING`               | `bigquery.NullString`    |
| `BOOL`                 | `bigquery.NullBool`      |
| `INTEGER`              | `bigquery.NullInt64`     |
| `FLOAT`                | `bigquery.NullFloat64`   |
| `TIMESTAMP`            | `bigquery.NullTimestamp` |
| `DATE`                 | `bigquery.NullDate`      |
| `TIME`                 | `bigquery.NullTime`      |
| `DATETIME`             | `bigquery.NullDateTime`  |
| `GEOGRAPHY`            | `bigquery.NullGeography` |
