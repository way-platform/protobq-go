package protobq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/genproto/googleapis/type/datetime"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/genproto/googleapis/type/timeofday"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// MessageLoader implements bigquery.ValueLoader for a proto.Message.
// The message is converted from a BigQuery row using the provided UnmarshalOptions.
type MessageLoader struct {
	// If DiscardUnknown is set, unknown fields are ignored.
	DiscardUnknown bool

	// Message to load.
	Message proto.Message
}

var _ bigquery.ValueLoader = &MessageLoader{}

// Load the bigquery.Value list into the given proto.Message using the given bigquery.Schema
// using options in UnmarshalOptions object.
// It will clear the message first before setting the fields. If it returns an error,
// the given message may be partially set.
func (o *MessageLoader) Load(bqMessage []bigquery.Value, bqSchema bigquery.Schema) error {
	proto.Reset(o.Message)
	if err := o.loadMessage(bqMessage, bqSchema, o.Message.ProtoReflect()); err != nil {
		return err
	}
	return nil
}

func (o *MessageLoader) loadMessage(
	bqMessage []bigquery.Value,
	bqSchema bigquery.Schema,
	message protoreflect.Message,
) error {
	if len(bqMessage) != len(bqSchema) {
		return fmt.Errorf("message has %d fields but schema has %d fields", len(bqMessage), len(bqSchema))
	}
	for i, bqFieldSchema := range bqSchema {
		bqField := bqMessage[i]
		fieldName := protoreflect.Name(bqFieldSchema.Name)
		field := message.Descriptor().Fields().ByName(fieldName)
		if field == nil {
			if !o.DiscardUnknown && !message.Descriptor().ReservedNames().Has(fieldName) {
				return fmt.Errorf("unknown field: %s", fieldName)
			}
			continue
		}
		switch {
		case field.IsList():
			if err := o.loadListField(bqField, bqFieldSchema, field, message); err != nil {
				return err
			}
		case field.IsMap():
			if err := o.loadMapField(bqField, bqFieldSchema, field, message); err != nil {
				return err
			}
		default:
			value, err := o.loadSingularField(bqField, bqFieldSchema, field, message)
			if err != nil {
				return err
			}
			if value.IsValid() {
				message.Set(field, value)
			}
		}
	}
	return nil
}

func (o *MessageLoader) loadListField(
	bqField bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	if !bqFieldSchema.Repeated {
		return fmt.Errorf("%s: unsupported field schema for list field: not repeated", field.Name())
	}
	bqList, ok := bqField.([]bigquery.Value)
	if !ok {
		return fmt.Errorf("%s: unsupported BigQuery value for message: %v", field.Name(), bqField)
	}
	isMessage := field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind
	switch {
	case isMessage && isWellKnownType(string(field.Message().FullName())):
		return o.unmarshalWellKnownTypeListField(bqList, field, message)
	case isMessage && bqFieldSchema.Type == bigquery.RangeFieldType:
		return o.unmarshalRangeListField(bqList, bqFieldSchema, field, message)
	case isMessage:
		return o.loadMessageListField(bqList, bqFieldSchema, field, message)
	default:
		return o.unmarshalScalarListField(bqList, field, message)
	}
}

func (o *MessageLoader) loadMessageListField(
	bqListValue []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	list := message.Mutable(field).List()
	for _, bqElement := range bqListValue {
		if bqFieldSchema.Type != bigquery.RecordFieldType {
			return fmt.Errorf(
				"%s: field schema has type %s but expected %s",
				field.Name(),
				bqFieldSchema.Type,
				bigquery.RecordFieldType,
			)
		}
		bqMessageElement, ok := bqElement.([]bigquery.Value)
		if !ok {
			return fmt.Errorf(
				"%s: unsupported BigQuery value for message: %v", field.Name(), bqMessageElement,
			)
		}
		listElementValue := list.NewElement()
		if err := o.loadMessage(bqMessageElement, bqFieldSchema.Schema, listElementValue.Message()); err != nil {
			return err
		}
		list.Append(listElementValue)
	}
	return nil
}

func (o *MessageLoader) loadMapField(
	bqField bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	bqMapField, ok := bqField.([]bigquery.Value)
	if !ok {
		return fmt.Errorf("%s: unsupported BigQuery value for message: %v", field.Name(), bqField)
	}
	mapValue := field.MapValue()
	isMessage := mapValue.Kind() == protoreflect.MessageKind || mapValue.Kind() == protoreflect.GroupKind
	switch {
	case isMessage && isWellKnownType(string(mapValue.Message().FullName())):
		return o.unmarshalWellKnownTypeValueMapField(bqMapField, bqFieldSchema, field, message)
	case isMessage && len(bqFieldSchema.Schema) >= 2 && bqFieldSchema.Schema[1].Type == bigquery.RangeFieldType:
		return o.unmarshalRangeValueMapField(bqMapField, bqFieldSchema, field, message)
	case isMessage:
		return o.loadMessageValueMapField(bqMapField, bqFieldSchema, field, message)
	default:
		return o.unmarshalScalarValueMapField(bqMapField, field, message)
	}
}

func (o *MessageLoader) loadMessageValueMapField(
	bqMapField []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	mapField := message.Mutable(field).Map()
	for _, bqMapEntry := range bqMapField {
		// Handle null/nil map entries
		if bqMapEntry == nil {
			continue
		}

		// Handle map format entries (object format)
		if entryMap, ok := bqMapEntry.(map[string]bigquery.Value); ok {
			if len(entryMap) == 0 {
				// Skip empty map entries
				continue
			}
			// Process non-empty map entry
			if err := o.processMapEntry(entryMap, bqFieldSchema, field, mapField); err != nil {
				return err
			}
		} else if entryArray, ok := bqMapEntry.([]bigquery.Value); ok {
			// Handle array format entries (BigQuery REPEATED RECORD format: [key, value])
			if len(entryArray) == 0 {
				// Skip empty array entries
				continue
			}
			if err := o.loadArrayMapEntry(entryArray, bqFieldSchema, field, mapField); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%s: unsupported BigQuery value for map entry: %v", field.Name(), bqMapEntry)
		}
	}
	return nil
}

func (o *MessageLoader) processMapEntry(
	bqMapEntry map[string]bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	mapField protoreflect.Map,
) error {
	mapEntryKey, err := o.unmarshalMapEntryKey(bqMapEntry)
	if err != nil {
		return err
	}
	bqMapEntryValue, ok := bqMapEntry["value"]
	if !ok {
		return fmt.Errorf("%s: map entry is missing value field", field.Name())
	}
	bqMapEntryMessageValue, ok := bqMapEntryValue.([]bigquery.Value)
	if !ok {
		return fmt.Errorf("%s: unsupported BigQuery value for message: %v", field.Name(), bqMapEntryValue)
	}
	if len(bqFieldSchema.Schema) != 2 || bqFieldSchema.Schema[1].Name != "value" {
		return fmt.Errorf("%s: unsupported BigQuery schema for map entry", field.Name())
	}
	bqMapEntryValueSchema := bqFieldSchema.Schema[1].Schema
	mapEntryValue := mapField.NewValue()
	if err := o.loadMessage(bqMapEntryMessageValue, bqMapEntryValueSchema, mapEntryValue.Message()); err != nil {
		return err
	}
	mapField.Set(mapEntryKey, mapEntryValue)
	return nil
}

func (o *MessageLoader) loadArrayMapEntry(
	bqMapEntryArray []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	mapField protoreflect.Map,
) error {
	// BigQuery REPEATED RECORD format: [key, value]
	// Expected schema: [{Name: "key", Type: STRING}, {Name: "value", Type: RECORD, Schema: [...]}]
	if len(bqFieldSchema.Schema) != 2 {
		return fmt.Errorf("%s: unsupported BigQuery schema for array-format map entry", field.Name())
	}
	if len(bqMapEntryArray) != 2 {
		return fmt.Errorf("%s: array-format map entry must have exactly 2 elements [key, value], got %d", field.Name(), len(bqMapEntryArray))
	}
	// Extract key (first element)
	bqMapEntryKey := bqMapEntryArray[0]
	mapEntryKey := protoreflect.ValueOf(bqMapEntryKey).MapKey()
	// Extract value (second element)
	bqMapEntryValue := bqMapEntryArray[1]
	bqMapEntryMessageValue, ok := bqMapEntryValue.([]bigquery.Value)
	if !ok {
		return fmt.Errorf("%s: unsupported BigQuery value for message in array-format entry: %v", field.Name(), bqMapEntryValue)
	}
	// Validate and extract value schema
	if bqFieldSchema.Schema[1].Name != "value" {
		return fmt.Errorf("%s: expected 'value' field in schema position 1 for array-format map entry", field.Name())
	}
	bqMapEntryValueSchema := bqFieldSchema.Schema[1].Schema
	// Load the message value
	mapEntryValue := mapField.NewValue()
	if err := o.loadMessage(bqMapEntryMessageValue, bqMapEntryValueSchema, mapEntryValue.Message()); err != nil {
		return err
	}
	mapField.Set(mapEntryKey, mapEntryValue)
	return nil
}

func (o *MessageLoader) loadSingularField(
	bqField bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) (protoreflect.Value, error) {
	if bqField == nil {
		return protoreflect.ValueOf(nil), nil
	}
	if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
		if isWellKnownType(string(field.Message().FullName())) {
			return o.unmarshalWellKnownTypeField(bqField, field)
		}
		// Handle BigQuery RANGE types
		if bqFieldSchema.Type == bigquery.RangeFieldType {
			return o.unmarshalRangeField(bqField, field, message)
		}
		if bqFieldSchema.Type != bigquery.RecordFieldType {
			return protoreflect.ValueOf(nil), fmt.Errorf(
				"%s: unsupported BigQuery type for message: %v", field.Name(), bqFieldSchema.Type,
			)
		}
		bqMessage, ok := bqField.([]bigquery.Value)
		if !ok {
			return protoreflect.ValueOf(nil), fmt.Errorf("unsupported BigQuery value for message: %v", bqMessage)
		}
		fieldValue := message.NewField(field)
		if err := o.loadMessage(bqMessage, bqFieldSchema.Schema, fieldValue.Message()); err != nil {
			return protoreflect.ValueOf(nil), fmt.Errorf("%s: %w", field.Name(), err)
		}
		return fieldValue, nil
	}
	return o.unmarshalScalar(bqField, bqFieldSchema, field)
}

func (o *MessageLoader) unmarshalWellKnownTypeListField(
	bqListValue []bigquery.Value,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	list := message.Mutable(field).List()
	for _, bqListElementValue := range bqListValue {
		value, err := o.unmarshalWellKnownTypeField(bqListElementValue, field)
		if err != nil {
			return err
		}
		list.Append(value)
	}
	return nil
}

func (o *MessageLoader) unmarshalRangeListField(
	bqListValue []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	list := message.Mutable(field).List()
	for _, bqListElementValue := range bqListValue {
		// Create a new list element for the range message
		listElementValue := list.NewElement()

		// Manually unmarshal the range into the list element message
		rangeValue, ok := bqListElementValue.(*bigquery.RangeValue)
		if !ok {
			return fmt.Errorf("unsupported BigQuery value for RANGE: %T", bqListElementValue)
		}

		rangeMessage := listElementValue.Message()
		messageName := string(field.Message().FullName())

		// Get field descriptors for start and end
		startField := rangeMessage.Descriptor().Fields().ByName("start")
		endField := rangeMessage.Descriptor().Fields().ByName("end")
		if startField == nil || endField == nil {
			return fmt.Errorf("invalid range message type: missing start or end field in %s", messageName)
		}

		// Handle start value
		if rangeValue.Start != nil {
			startValue, err := o.unmarshalRangeValue(rangeValue.Start, startField, messageName)
			if err != nil {
				return fmt.Errorf("error unmarshaling range start: %w", err)
			}
			if startValue.IsValid() {
				rangeMessage.Set(startField, startValue)
			}
		}

		// Handle end value
		if rangeValue.End != nil {
			endValue, err := o.unmarshalRangeValue(rangeValue.End, endField, messageName)
			if err != nil {
				return fmt.Errorf("error unmarshaling range end: %w", err)
			}
			if endValue.IsValid() {
				rangeMessage.Set(endField, endValue)
			}
		}

		list.Append(listElementValue)
	}
	return nil
}

func (o *MessageLoader) unmarshalScalarListField(
	bqListValue []bigquery.Value,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	list := message.Mutable(field).List()
	for _, bqListElementValue := range bqListValue {
		value, err := o.unmarshalScalar(bqListElementValue, nil, field)
		if err != nil {
			return err
		}
		list.Append(value)
	}
	return nil
}

func (o *MessageLoader) unmarshalScalarValueMapField(
	bqMapField []bigquery.Value,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	mapField := message.Mutable(field).Map()
	for _, bqMapEntry := range bqMapField {
		// Handle null/nil map entries
		if bqMapEntry == nil {
			continue
		}

		// Handle map format entries (object format)
		if entryMap, ok := bqMapEntry.(map[string]bigquery.Value); ok {
			if len(entryMap) == 0 {
				// Skip empty map entries
				continue
			}
			// Process non-empty map entry
			mapEntryKey, err := o.unmarshalMapEntryKey(entryMap)
			if err != nil {
				return err
			}
			bqMapEntryValue, ok := entryMap["value"]
			if !ok {
				return fmt.Errorf("%s: map entry is missing value field", field.Name())
			}
			mapEntryValue, err := o.unmarshalScalar(bqMapEntryValue, nil, field.MapValue())
			if err != nil {
				return err
			}
			mapField.Set(mapEntryKey, mapEntryValue)
		} else if entryArray, ok := bqMapEntry.([]bigquery.Value); ok {
			// Handle array format entries (BigQuery REPEATED RECORD format: [key, value])
			if len(entryArray) == 0 {
				// Skip empty array entries
				continue
			}
			if err := o.processArrayScalarMapEntry(entryArray, field, mapField); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%s: unsupported BigQuery value for map entry: %v", field.Name(), bqMapEntry)
		}
	}
	return nil
}

func (o *MessageLoader) processArrayScalarMapEntry(
	bqMapEntryArray []bigquery.Value,
	field protoreflect.FieldDescriptor,
	mapField protoreflect.Map,
) error {
	// BigQuery REPEATED RECORD format for scalar values: [key, value]
	if len(bqMapEntryArray) != 2 {
		return fmt.Errorf("%s: array-format map entry must have exactly 2 elements [key, value], got %d", field.Name(), len(bqMapEntryArray))
	}

	// Extract key (first element)
	bqMapEntryKey := bqMapEntryArray[0]
	mapEntryKey := protoreflect.ValueOf(bqMapEntryKey).MapKey()

	// Extract value (second element)
	bqMapEntryValue := bqMapEntryArray[1]
	mapEntryValue, err := o.unmarshalScalar(bqMapEntryValue, nil, field.MapValue())
	if err != nil {
		return err
	}

	mapField.Set(mapEntryKey, mapEntryValue)
	return nil
}

func (o *MessageLoader) unmarshalRangeValueMapField(
	bqMapField []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	mapField := message.Mutable(field).Map()
	for _, bqMapEntry := range bqMapField {
		// Handle null/nil map entries
		if bqMapEntry == nil {
			continue
		}

		// Handle map format entries (object format)
		if entryMap, ok := bqMapEntry.(map[string]bigquery.Value); ok {
			if len(entryMap) == 0 {
				// Skip empty map entries
				continue
			}
			// Process non-empty map entry
			mapEntryKey, err := o.unmarshalMapEntryKey(entryMap)
			if err != nil {
				return err
			}
			bqMapEntryValue, ok := entryMap["value"]
			if !ok {
				return fmt.Errorf("%s: map entry is missing value field", field.Name())
			}

			// Handle Range type in map value - create new value and unmarshal range into it
			mapEntryValue := mapField.NewValue()
			rangeValue, ok := bqMapEntryValue.(*bigquery.RangeValue)
			if !ok {
				return fmt.Errorf("unsupported BigQuery value for RANGE: %T", bqMapEntryValue)
			}

			rangeMessage := mapEntryValue.Message()
			messageName := string(field.MapValue().Message().FullName())

			// Get field descriptors for start and end
			startField := rangeMessage.Descriptor().Fields().ByName("start")
			endField := rangeMessage.Descriptor().Fields().ByName("end")
			if startField == nil || endField == nil {
				return fmt.Errorf("invalid range message type: missing start or end field in %s", messageName)
			}

			// Handle start value
			if rangeValue.Start != nil {
				startValue, err := o.unmarshalRangeValue(rangeValue.Start, startField, messageName)
				if err != nil {
					return fmt.Errorf("error unmarshaling range start: %w", err)
				}
				if startValue.IsValid() {
					rangeMessage.Set(startField, startValue)
				}
			}

			// Handle end value
			if rangeValue.End != nil {
				endValue, err := o.unmarshalRangeValue(rangeValue.End, endField, messageName)
				if err != nil {
					return fmt.Errorf("error unmarshaling range end: %w", err)
				}
				if endValue.IsValid() {
					rangeMessage.Set(endField, endValue)
				}
			}

			mapField.Set(mapEntryKey, mapEntryValue)
		} else if entryArray, ok := bqMapEntry.([]bigquery.Value); ok {
			// Handle array format entries (BigQuery REPEATED RECORD format: [key, value])
			if len(entryArray) == 0 {
				// Skip empty array entries
				continue
			}
			if err := o.processArrayRangeMapEntry(entryArray, bqFieldSchema, field, mapField); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%s: unsupported BigQuery value for map entry: %v", field.Name(), bqMapEntry)
		}
	}
	return nil
}

func (o *MessageLoader) processArrayRangeMapEntry(
	bqMapEntryArray []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	mapField protoreflect.Map,
) error {
	// BigQuery REPEATED RECORD format for range values: [key, range_value]
	if len(bqMapEntryArray) != 2 {
		return fmt.Errorf("%s: array-format map entry must have exactly 2 elements [key, value], got %d", field.Name(), len(bqMapEntryArray))
	}

	// Extract key (first element)
	bqMapEntryKey := bqMapEntryArray[0]
	mapEntryKey := protoreflect.ValueOf(bqMapEntryKey).MapKey()

	// Extract value (second element) - should be a RangeValue
	bqMapEntryValue := bqMapEntryArray[1]
	rangeValue, ok := bqMapEntryValue.(*bigquery.RangeValue)
	if !ok {
		return fmt.Errorf("unsupported BigQuery value for RANGE in array-format entry: %T", bqMapEntryValue)
	}

	// Handle Range type - create new value and unmarshal range into it
	mapEntryValue := mapField.NewValue()
	rangeMessage := mapEntryValue.Message()
	messageName := string(field.MapValue().Message().FullName())

	// Get field descriptors for start and end
	startField := rangeMessage.Descriptor().Fields().ByName("start")
	endField := rangeMessage.Descriptor().Fields().ByName("end")
	if startField == nil || endField == nil {
		return fmt.Errorf("invalid range message type: missing start or end field in %s", messageName)
	}

	// Handle start value
	if rangeValue.Start != nil {
		startValue, err := o.unmarshalRangeValue(rangeValue.Start, startField, messageName)
		if err != nil {
			return fmt.Errorf("error unmarshaling range start: %w", err)
		}
		if startValue.IsValid() {
			rangeMessage.Set(startField, startValue)
		}
	}

	// Handle end value
	if rangeValue.End != nil {
		endValue, err := o.unmarshalRangeValue(rangeValue.End, endField, messageName)
		if err != nil {
			return fmt.Errorf("error unmarshaling range end: %w", err)
		}
		if endValue.IsValid() {
			rangeMessage.Set(endField, endValue)
		}
	}

	mapField.Set(mapEntryKey, mapEntryValue)
	return nil
}

func (o *MessageLoader) unmarshalWellKnownTypeValueMapField(
	bqMapField []bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
	message protoreflect.Message,
) error {
	mapField := message.Mutable(field).Map()
	for _, bqMapEntry := range bqMapField {
		// Handle null/nil map entries
		if bqMapEntry == nil {
			continue
		}

		// Handle map format entries (object format)
		if entryMap, ok := bqMapEntry.(map[string]bigquery.Value); ok {
			if len(entryMap) == 0 {
				// Skip empty map entries
				continue
			}
			// Process non-empty map entry
			mapEntryKey, err := o.unmarshalMapEntryKey(entryMap)
			if err != nil {
				return err
			}
			bqMapEntryValue, ok := entryMap["value"]
			if !ok {
				return fmt.Errorf("%s: map entry is missing value field", field.Name())
			}

			// Handle regular well-known type
			mapEntryValue, err := o.unmarshalWellKnownTypeField(bqMapEntryValue, field.MapValue())
			if err != nil {
				return err
			}
			mapField.Set(mapEntryKey, mapEntryValue)
		} else if entryArray, ok := bqMapEntry.([]bigquery.Value); ok {
			// Handle array format entries (BigQuery REPEATED RECORD format: [key, value])
			if len(entryArray) == 0 {
				// Skip empty array entries
				continue
			}
			if err := o.processArrayWellKnownTypeMapEntry(entryArray, field, mapField); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("%s: unsupported BigQuery value for map entry: %v", field.Name(), bqMapEntry)
		}
	}
	return nil
}

func (o *MessageLoader) processArrayWellKnownTypeMapEntry(
	bqMapEntryArray []bigquery.Value,
	field protoreflect.FieldDescriptor,
	mapField protoreflect.Map,
) error {
	// BigQuery REPEATED RECORD format for well-known type values: [key, value]
	if len(bqMapEntryArray) != 2 {
		return fmt.Errorf("%s: array-format map entry must have exactly 2 elements [key, value], got %d", field.Name(), len(bqMapEntryArray))
	}

	// Extract key (first element)
	bqMapEntryKey := bqMapEntryArray[0]
	mapEntryKey := protoreflect.ValueOf(bqMapEntryKey).MapKey()

	// Extract value (second element)
	bqMapEntryValue := bqMapEntryArray[1]

	// Handle well-known type
	mapEntryValue, err := o.unmarshalWellKnownTypeField(bqMapEntryValue, field.MapValue())
	if err != nil {
		return err
	}

	mapField.Set(mapEntryKey, mapEntryValue)
	return nil
}

func (o *MessageLoader) unmarshalMapEntryKey(
	bqMapEntry map[string]bigquery.Value,
) (protoreflect.MapKey, error) {
	bqMapEntryKey, ok := bqMapEntry["key"]
	if !ok {
		return protoreflect.MapKey{}, fmt.Errorf("map entry is missing key field")
	}
	return protoreflect.ValueOf(bqMapEntryKey).MapKey(), nil
}

func (o *MessageLoader) unmarshalWellKnownTypeField(
	bqValue bigquery.Value,
	field protoreflect.FieldDescriptor,
) (protoreflect.Value, error) {
	var result proto.Message
	var err error
	switch field.Message().FullName() {
	case wktTimestamp:
		result, err = o.unmarshalTimestamp(bqValue)
	case wktDuration:
		result, err = o.unmarshalDuration(bqValue)
	case wktTimeOfDay:
		result, err = o.unmarshalTimeOfDay(bqValue)
	case wktDate:
		result, err = o.unmarshalDate(bqValue)
	case kwtDateTime:
		result, err = o.unmarshalDateTime(bqValue)
	case wktLatLng:
		result, err = o.unmarshalLatLng(bqValue)
	case wktStruct:
		result, err = o.unmarshalStruct(bqValue)
	case wktDoubleValue:
		result, err = o.unmarshalDoubleValue(bqValue)
	case wktFloatValue:
		result, err = o.unmarshalFloatValue(bqValue)
	case wktInt32Value:
		result, err = o.unmarshalInt32Value(bqValue)
	case wktInt64Value:
		result, err = o.unmarshalInt64Value(bqValue)
	case wktUInt32Value:
		result, err = o.unmarshalUInt32Value(bqValue)
	case wktUInt64Value:
		result, err = o.unmarshalUInt64Value(bqValue)
	case wktBoolValue:
		result, err = o.unmarshalBoolValue(bqValue)
	case wktStringValue:
		result, err = o.unmarshalStringValue(bqValue)
	case wktBytesValue:
		result, err = o.unmarshalBytesValue(bqValue)
	default:
		result, err = nil, fmt.Errorf("unsupported well-known-type: %s", field.Message().FullName())
	}
	if err != nil {
		return protoreflect.ValueOf(nil), err
	}
	return protoreflect.ValueOf(result.ProtoReflect()), nil
}

func (o *MessageLoader) unmarshalTimestamp(bqValue bigquery.Value) (*timestamppb.Timestamp, error) {
	switch v := bqValue.(type) {
	case time.Time:
		return timestamppb.New(v), nil
	case string:
		// Parse RFC3339 string
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return nil, fmt.Errorf("invalid RFC3339 timestamp string for %s: %v: %w", wktTimestamp, v, err)
		}
		return timestamppb.New(t), nil
	case int64:
		// Assume microseconds since Unix epoch (BigQuery TIMESTAMP format)
		return timestamppb.New(time.UnixMicro(v)), nil
	case int32:
		// Assume seconds since Unix epoch
		return timestamppb.New(time.Unix(int64(v), 0)), nil
	case uint32:
		// Assume seconds since Unix epoch
		return timestamppb.New(time.Unix(int64(v), 0)), nil
	default:
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %v", wktTimestamp, bqValue)
	}
}

func (o *MessageLoader) unmarshalDuration(bqValue bigquery.Value) (*durationpb.Duration, error) {
	var duration time.Duration
	switch v := bqValue.(type) {
	case int64:
		duration = time.Duration(v) * time.Second
	case float64:
		duration = time.Duration(v * float64(time.Second))
	case string:
		// Try to parse various string formats
		var err error

		// First try ISO8601 duration format (PT1H30M45.123S)
		if duration, err = parseISO8601Duration(v); err == nil {
			return durationpb.New(duration), nil
		}

		// Try BigQuery interval format (H:MM:SS or H:MM:SS.sss)
		if duration, err = parseBigQueryInterval(v); err == nil {
			return durationpb.New(duration), nil
		}

		// If both fail, return error
		return nil, fmt.Errorf("invalid duration string for %s: %v (tried ISO8601 and BigQuery interval formats)", wktDuration, v)
	default:
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", wktDuration, bqValue)
	}
	return durationpb.New(duration), nil
}

func (o *MessageLoader) unmarshalTimeOfDay(bqValue bigquery.Value) (*timeofday.TimeOfDay, error) {
	t, ok := bqValue.(civil.Time)
	if !ok {
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", wktTimeOfDay, bqValue)
	}
	return &timeofday.TimeOfDay{
		Hours:   int32(t.Hour),
		Minutes: int32(t.Minute),
		Seconds: int32(t.Second),
		Nanos:   int32(t.Nanosecond),
	}, nil
}

func (o *MessageLoader) unmarshalDate(bqValue bigquery.Value) (*date.Date, error) {
	d, ok := bqValue.(civil.Date)
	if !ok {
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", wktDate, bqValue)
	}
	return &date.Date{
		Year:  int32(d.Year),
		Month: int32(d.Month),
		Day:   int32(d.Day),
	}, nil
}

func (o *MessageLoader) unmarshalDateTime(bqValue bigquery.Value) (*datetime.DateTime, error) {
	switch v := bqValue.(type) {
	case civil.DateTime:
		return &datetime.DateTime{
			Year:    int32(v.Date.Year),
			Month:   int32(v.Date.Month),
			Day:     int32(v.Date.Day),
			Hours:   int32(v.Time.Hour),
			Minutes: int32(v.Time.Minute),
			Seconds: int32(v.Time.Second),
			Nanos:   int32(v.Time.Nanosecond),
		}, nil
	case time.Time:
		name, offset := v.Zone()
		var timeZone *datetime.TimeZone
		if name != "" {
			// Named timezone (e.g., "UTC", "PST")
			timeZone = &datetime.TimeZone{Id: name}
		} else {
			// Numeric offset timezone (e.g., "+08:00", "-05:00")
			var offsetStr string
			if offset >= 0 {
				offsetHours := offset / 3600
				offsetMinutes := (offset % 3600) / 60
				offsetStr = fmt.Sprintf("+%02d:%02d", offsetHours, offsetMinutes)
			} else {
				// For negative offsets, make sure we handle the sign correctly
				absOffset := -offset
				offsetHours := absOffset / 3600
				offsetMinutes := (absOffset % 3600) / 60
				offsetStr = fmt.Sprintf("-%02d:%02d", offsetHours, offsetMinutes)
			}
			timeZone = &datetime.TimeZone{Id: offsetStr}
		}
		return &datetime.DateTime{
			Year:    int32(v.Year()),
			Month:   int32(v.Month()),
			Day:     int32(v.Day()),
			Hours:   int32(v.Hour()),
			Minutes: int32(v.Minute()),
			Seconds: int32(v.Second()),
			Nanos:   int32(v.Nanosecond()),
			TimeOffset: &datetime.DateTime_TimeZone{
				TimeZone: timeZone,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", kwtDateTime, bqValue)
	}
}

func (o *MessageLoader) unmarshalLatLng(bqValue bigquery.Value) (*latlng.LatLng, error) {
	s, ok := bqValue.(string)
	if !ok {
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", wktLatLng, bqValue)
	}
	latLng := &latlng.LatLng{}
	if _, err := fmt.Sscanf(s, "POINT(%f %f)", &latLng.Longitude, &latLng.Latitude); err != nil {
		return nil, fmt.Errorf("invalid GEOGRAPHY value for %s: %#v: %w", wktLatLng, bqValue, err)
	}
	return latLng, nil
}

func (o *MessageLoader) unmarshalStruct(bqValue bigquery.Value) (*structpb.Struct, error) {
	s, ok := bqValue.(string)
	if !ok {
		return nil, fmt.Errorf("unsupported BigQuery value for %s: %#v", wktStruct, bqValue)
	}
	var structValue structpb.Struct
	if err := structValue.UnmarshalJSON([]byte(s)); err != nil {
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v: %w", wktStruct, bqValue, err)
	}
	return &structValue, nil
}

func (o *MessageLoader) unmarshalDoubleValue(bqValue bigquery.Value) (*wrapperspb.DoubleValue, error) {
	switch bqValue := bqValue.(type) {
	case float32:
		return wrapperspb.Double(float64(bqValue)), nil
	case float64:
		return wrapperspb.Double(bqValue), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktDoubleValue, bqValue)
	}
}

func (o *MessageLoader) unmarshalFloatValue(bqValue bigquery.Value) (*wrapperspb.FloatValue, error) {
	switch bqValue := bqValue.(type) {
	case float32:
		return wrapperspb.Float(bqValue), nil
	case float64:
		return wrapperspb.Float(float32(bqValue)), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktFloatValue, bqValue)
	}
}

func (o *MessageLoader) unmarshalInt32Value(bqValue bigquery.Value) (*wrapperspb.Int32Value, error) {
	switch bqValue := bqValue.(type) {
	case int32:
		return wrapperspb.Int32(bqValue), nil
	case int64:
		return wrapperspb.Int32(int32(bqValue)), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktInt32Value, bqValue)
	}
}

func (o *MessageLoader) unmarshalInt64Value(bqValue bigquery.Value) (*wrapperspb.Int64Value, error) {
	switch bqValue := bqValue.(type) {
	case int32:
		return wrapperspb.Int64(int64(bqValue)), nil
	case int64:
		return wrapperspb.Int64(bqValue), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktInt64Value, bqValue)
	}
}

func (o *MessageLoader) unmarshalUInt32Value(bqValue bigquery.Value) (*wrapperspb.UInt32Value, error) {
	switch bqValue := bqValue.(type) {
	case uint32:
		return wrapperspb.UInt32(bqValue), nil
	case uint64:
		return wrapperspb.UInt32(uint32(bqValue)), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktUInt32Value, bqValue)
	}
}

func (o *MessageLoader) unmarshalUInt64Value(bqValue bigquery.Value) (*wrapperspb.UInt64Value, error) {
	switch bqValue := bqValue.(type) {
	case uint32:
		return wrapperspb.UInt64(uint64(bqValue)), nil
	case uint64:
		return wrapperspb.UInt64(bqValue), nil
	default:
		return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktUInt64Value, bqValue)
	}
}

func (o *MessageLoader) unmarshalBoolValue(bqValue bigquery.Value) (*wrapperspb.BoolValue, error) {
	if bqValue, ok := bqValue.(bool); ok {
		return wrapperspb.Bool(bqValue), nil
	}
	return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktBoolValue, bqValue)
}

func (o *MessageLoader) unmarshalStringValue(bqValue bigquery.Value) (*wrapperspb.StringValue, error) {
	if bqValue, ok := bqValue.(string); ok {
		return wrapperspb.String(bqValue), nil
	}
	return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktStringValue, bqValue)
}

func (o *MessageLoader) unmarshalBytesValue(bqValue bigquery.Value) (*wrapperspb.BytesValue, error) {
	if bqValue, ok := bqValue.([]byte); ok {
		return wrapperspb.Bytes(bqValue), nil
	}
	return nil, fmt.Errorf("invalid BigQuery value for %s: %#v", wktBytesValue, bqValue)
}

func (o *MessageLoader) unmarshalRangeField(bqValue bigquery.Value, field protoreflect.FieldDescriptor, message protoreflect.Message) (protoreflect.Value, error) {
	rangeValue, ok := bqValue.(*bigquery.RangeValue)
	if !ok {
		return protoreflect.ValueOf(nil), fmt.Errorf("unsupported BigQuery value for RANGE: %T", bqValue)
	}
	// Create a new instance of the range message type
	fieldValue := message.NewField(field)
	rangeMessage := fieldValue.Message()
	// Get the message type name to determine how to handle start/end values
	messageName := string(field.Message().FullName())
	// Get field descriptors for start and end
	startField := rangeMessage.Descriptor().Fields().ByName("start")
	endField := rangeMessage.Descriptor().Fields().ByName("end")
	if startField == nil || endField == nil {
		return protoreflect.ValueOf(nil), fmt.Errorf("invalid range message type: missing start or end field in %s", messageName)
	}
	// Handle start value
	if rangeValue.Start != nil {
		startValue, err := o.unmarshalRangeValue(rangeValue.Start, startField, messageName)
		if err != nil {
			return protoreflect.ValueOf(nil), fmt.Errorf("error unmarshaling range start: %w", err)
		}
		if startValue.IsValid() {
			rangeMessage.Set(startField, startValue)
		}
	}
	// Handle end value
	if rangeValue.End != nil {
		endValue, err := o.unmarshalRangeValue(rangeValue.End, endField, messageName)
		if err != nil {
			return protoreflect.ValueOf(nil), fmt.Errorf("error unmarshaling range end: %w", err)
		}
		if endValue.IsValid() {
			rangeMessage.Set(endField, endValue)
		}
	}
	return fieldValue, nil
}

func (o *MessageLoader) unmarshalRangeValue(bqValue bigquery.Value, field protoreflect.FieldDescriptor, messageName string) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		// For DateRange and DateTimeRange, convert the value to string
		switch v := bqValue.(type) {
		case string:
			return protoreflect.ValueOfString(v), nil
		case time.Time:
			// For DATE ranges, format as YYYY-MM-DD
			if strings.Contains(messageName, "DateRange") {
				return protoreflect.ValueOfString(v.Format("2006-01-02")), nil
			}
			// For DATETIME ranges, format as YYYY-MM-DD HH:MM:SS[.ffffff]
			return protoreflect.ValueOfString(v.Format("2006-01-02 15:04:05.999999")), nil
		default:
			return protoreflect.ValueOf(nil), fmt.Errorf("unsupported value type for string range field: %T", bqValue)
		}
	case protoreflect.MessageKind:
		// For TimestampRange, create a google.protobuf.Timestamp
		if strings.Contains(string(field.Message().FullName()), "Timestamp") {
			switch v := bqValue.(type) {
			case time.Time:
				timestamp, err := o.unmarshalTimestamp(v)
				if err != nil {
					return protoreflect.ValueOf(nil), err
				}
				return protoreflect.ValueOf(timestamp.ProtoReflect()), nil
			default:
				return protoreflect.ValueOf(nil), fmt.Errorf("unsupported value type for timestamp range field: %T", bqValue)
			}
		}
		return protoreflect.ValueOf(nil), fmt.Errorf("unsupported message type for range field: %s", field.Message().FullName())
	default:
		return protoreflect.ValueOf(nil), fmt.Errorf("unsupported field kind for range value: %s", field.Kind())
	}
}

func (o *MessageLoader) unmarshalScalar(
	bqValue bigquery.Value,
	bqFieldSchema *bigquery.FieldSchema,
	field protoreflect.FieldDescriptor,
) (protoreflect.Value, error) {
	// Handle null values by returning zero values for scalar types
	if bqValue == nil {
		return o.zeroValueForFieldSchema(field)
	}

	// Handle special BigQuery field types that require validation
	if bqFieldSchema != nil {
		switch bqFieldSchema.Type {
		case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
			// For NUMERIC/BIGNUMERIC, convert string values to appropriate types
			if str, ok := bqValue.(string); ok {
				return o.parseNumericString(str, field)
			}
		case bigquery.JSONFieldType:
			// For JSON fields, validate that the string is valid JSON
			if str, ok := bqValue.(string); ok {
				if err := o.validateJSONString(str); err != nil {
					return protoreflect.Value{}, err
				}
			}
		case bigquery.DateFieldType:
			// For DATE fields with string values, validate the date format
			if str, ok := bqValue.(string); ok {
				if err := o.validateDateString(str); err != nil {
					return protoreflect.Value{}, err
				}
			}
		}
	}

	switch field.Kind() {
	case protoreflect.BoolKind:
		if b, ok := bqValue.(bool); ok {
			return protoreflect.ValueOfBool(b), nil
		}

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if n, ok := bqValue.(int64); ok {
			return protoreflect.ValueOfInt32(int32(n)), nil
		}

	case protoreflect.Int64Kind:
		switch v := bqValue.(type) {
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case time.Time:
			return protoreflect.ValueOfInt64(v.UnixMicro()), nil
		}

	case protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if n, ok := bqValue.(int64); ok {
			return protoreflect.ValueOfInt64(n), nil
		}

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if n, ok := bqValue.(int64); ok {
			return protoreflect.ValueOfUint32(uint32(n)), nil
		}

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if n, ok := bqValue.(int64); ok {
			return protoreflect.ValueOfUint64(uint64(n)), nil
		}

	case protoreflect.FloatKind:
		if n, ok := bqValue.(float64); ok {
			return protoreflect.ValueOfFloat32(float32(n)), nil
		}

	case protoreflect.DoubleKind:
		if n, ok := bqValue.(float64); ok {
			return protoreflect.ValueOfFloat64(n), nil
		}

	case protoreflect.StringKind:
		switch v := bqValue.(type) {
		case string:
			return protoreflect.ValueOfString(v), nil
		case time.Time:
			return protoreflect.ValueOfString(v.Format(time.RFC3339Nano)), nil
		}

	case protoreflect.BytesKind:
		if b, ok := bqValue.([]byte); ok {
			return protoreflect.ValueOfBytes(b), nil
		}

	case protoreflect.EnumKind:
		return o.unmarshalEnumScalar(bqValue, field)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		// Fall through to return error, these should have been handled by the caller.
	}
	return protoreflect.Value{}, fmt.Errorf("invalid BigQuery value %#v for kind %v", bqValue, field.Kind())
}

func (o *MessageLoader) unmarshalEnumScalar(
	bqValue bigquery.Value,
	field protoreflect.FieldDescriptor,
) (protoreflect.Value, error) {
	switch v := bqValue.(type) {
	case int64:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(v))), nil
	case string:
		enumVal := field.Enum().Values().ByName(protoreflect.Name(v))
		if enumVal == nil {
			return protoreflect.Value{}, fmt.Errorf(
				"unknown enum value %#v for enum %s", bqValue, field.Enum().FullName(),
			)
		}
		return protoreflect.ValueOfEnum(enumVal.Number()), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("invalid BigQuery value %#v for enum %s", bqValue, field.Enum().FullName())
	}
}

const (
	wktTimestamp   = "google.protobuf.Timestamp"
	wktDuration    = "google.protobuf.Duration"
	wktStruct      = "google.protobuf.Struct"
	wktTimeOfDay   = "google.type.TimeOfDay"
	wktDate        = "google.type.Date"
	kwtDateTime    = "google.type.DateTime"
	wktLatLng      = "google.type.LatLng"
	wktDoubleValue = "google.protobuf.DoubleValue"
	wktFloatValue  = "google.protobuf.FloatValue"
	wktInt32Value  = "google.protobuf.Int32Value"
	wktInt64Value  = "google.protobuf.Int64Value"
	wktUInt32Value = "google.protobuf.UInt32Value"
	wktUInt64Value = "google.protobuf.UInt64Value"
	wktBoolValue   = "google.protobuf.BoolValue"
	wktStringValue = "google.protobuf.StringValue"
	wktBytesValue  = "google.protobuf.BytesValue"
)

func isWellKnownType(t string) bool {
	switch t {
	case wktTimestamp,
		wktDuration,
		wktStruct,
		wktTimeOfDay,
		wktDate,
		kwtDateTime,
		wktLatLng,
		wktDoubleValue,
		wktFloatValue,
		wktInt32Value,
		wktInt64Value,
		wktUInt32Value,
		wktUInt64Value,
		wktBoolValue,
		wktStringValue,
		wktBytesValue:
		return true
	default:
		return false
	}
}

// zeroValueForFieldSchema returns the zero value for a protobuf field based on its kind
func (o *MessageLoader) zeroValueForFieldSchema(field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(false), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(0), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(0), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(0), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(0), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(0), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(0), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(""), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{}), nil
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(0), nil
	default:
		return protoreflect.Value{}, nil
	}
}

// validateJSONString validates that a string contains valid JSON
func (o *MessageLoader) validateJSONString(str string) error {
	var jsonObj interface{}
	if err := json.Unmarshal([]byte(str), &jsonObj); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

// validateDateString validates that a string is in a valid date format using civil.Date
func (o *MessageLoader) validateDateString(str string) error {
	if _, err := civil.ParseDate(str); err != nil {
		return fmt.Errorf("invalid date: %w", err)
	}
	return nil
}

// parseNumericString parses a NUMERIC or BIGNUMERIC string value into the appropriate protobuf type
func (o *MessageLoader) parseNumericString(str string, field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.DoubleKind:
		if f, err := strconv.ParseFloat(str, 64); err == nil {
			return protoreflect.ValueOfFloat64(f), nil
		}
	case protoreflect.FloatKind:
		if f, err := strconv.ParseFloat(str, 32); err == nil {
			return protoreflect.ValueOfFloat32(float32(f)), nil
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		if i, err := strconv.ParseInt(str, 10, 32); err == nil {
			return protoreflect.ValueOfInt32(int32(i)), nil
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if i, err := strconv.ParseInt(str, 10, 64); err == nil {
			return protoreflect.ValueOfInt64(i), nil
		}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if u, err := strconv.ParseUint(str, 10, 32); err == nil {
			return protoreflect.ValueOfUint32(uint32(u)), nil
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if u, err := strconv.ParseUint(str, 10, 64); err == nil {
			return protoreflect.ValueOfUint64(u), nil
		}
	case protoreflect.StringKind:
		// For string fields, just pass through the numeric string
		return protoreflect.ValueOfString(str), nil
	case protoreflect.BytesKind:
		// For bytes fields, return the string as bytes
		return protoreflect.ValueOfBytes([]byte(str)), nil
	}
	return protoreflect.Value{}, fmt.Errorf("cannot convert NUMERIC string %q to protobuf kind %v", str, field.Kind())
}

// parseBigQueryInterval parses BigQuery interval format (H:MM:SS or H:MM:SS.sss)
func parseBigQueryInterval(s string) (time.Duration, error) {
	// Split by colons
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid BigQuery interval format: expected H:MM:SS")
	}
	// Parse hours
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid hours in BigQuery interval: %s", parts[0])
	}
	// Parse minutes
	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid minutes in BigQuery interval: %s", parts[1])
	}
	// Parse seconds (may include fractional part)
	var seconds int64
	var nanoseconds int64
	secPart := parts[2]
	if dotIdx := strings.Index(secPart, "."); dotIdx >= 0 {
		// Handle fractional seconds
		if sec, err := strconv.ParseInt(secPart[:dotIdx], 10, 64); err == nil {
			seconds = sec
		} else {
			return 0, fmt.Errorf("invalid seconds in BigQuery interval: %s", secPart[:dotIdx])
		}
		fracPart := secPart[dotIdx+1:]
		if len(fracPart) > 0 {
			// Convert fractional part to nanoseconds
			for len(fracPart) < 9 {
				fracPart += "0"
			}
			if len(fracPart) > 9 {
				fracPart = fracPart[:9]
			}
			if nanos, err := strconv.ParseInt(fracPart, 10, 64); err == nil {
				nanoseconds = nanos
			} else {
				return 0, fmt.Errorf("invalid fractional seconds in BigQuery interval: %s", fracPart)
			}
		}
	} else {
		if sec, err := strconv.ParseInt(secPart, 10, 64); err == nil {
			seconds = sec
		} else {
			return 0, fmt.Errorf("invalid seconds in BigQuery interval: %s", secPart)
		}
	}
	duration := time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second +
		time.Duration(nanoseconds)*time.Nanosecond
	return duration, nil
}

// parseISO8601Duration parses ISO8601 duration format (PT1H30M45.123S)
func parseISO8601Duration(s string) (time.Duration, error) {
	// Basic ISO8601 parser for common cases
	// Format: PT[nH][nM][n[.n]S]
	if !strings.HasPrefix(s, "PT") {
		return 0, fmt.Errorf("invalid ISO8601 duration: must start with PT")
	}
	s = s[2:] // Remove "PT" prefix
	var hours, minutes, seconds, nanoseconds int64
	// Parse hours
	if idx := strings.Index(s, "H"); idx >= 0 {
		if h, err := strconv.ParseInt(s[:idx], 10, 64); err == nil {
			hours = h
			s = s[idx+1:]
		} else {
			return 0, fmt.Errorf("invalid hours in ISO8601 duration: %s", s[:idx])
		}
	}
	// Parse minutes
	if idx := strings.Index(s, "M"); idx >= 0 {
		if m, err := strconv.ParseInt(s[:idx], 10, 64); err == nil {
			minutes = m
			s = s[idx+1:]
		} else {
			return 0, fmt.Errorf("invalid minutes in ISO8601 duration: %s", s[:idx])
		}
	}
	// Parse seconds
	if idx := strings.Index(s, "S"); idx >= 0 {
		secStr := s[:idx]
		if dotIdx := strings.Index(secStr, "."); dotIdx >= 0 {
			// Handle fractional seconds
			secPart := secStr[:dotIdx]
			fracPart := secStr[dotIdx+1:]
			if sec, err := strconv.ParseInt(secPart, 10, 64); err == nil {
				seconds = sec
			} else {
				return 0, fmt.Errorf("invalid seconds in ISO8601 duration: %s", secPart)
			}
			// Convert fractional part to nanoseconds
			if len(fracPart) > 0 {
				// Pad or truncate to 9 digits (nanoseconds)
				for len(fracPart) < 9 {
					fracPart += "0"
				}
				if len(fracPart) > 9 {
					fracPart = fracPart[:9]
				}
				if nanos, err := strconv.ParseInt(fracPart, 10, 64); err == nil {
					nanoseconds = nanos
				} else {
					return 0, fmt.Errorf("invalid fractional seconds in ISO8601 duration: %s", fracPart)
				}
			}
		} else {
			if sec, err := strconv.ParseInt(secStr, 10, 64); err == nil {
				seconds = sec
			} else {
				return 0, fmt.Errorf("invalid seconds in ISO8601 duration: %s", secStr)
			}
		}
	}
	duration := time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second +
		time.Duration(nanoseconds)*time.Nanosecond
	return duration, nil
}
