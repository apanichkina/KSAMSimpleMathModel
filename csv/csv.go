package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type valueWithName struct {
	reflect.Value
	name string
}

func getCSVTypeName(field reflect.StructField) string {
	tags := field.Tag.Get("csv")
	parts := strings.Split(tags, ",")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return field.Name
}

func DeepFields(object interface{}) []valueWithName {
	fields := make([]valueWithName, 0)
	ifv := reflect.ValueOf(object)
	ift := reflect.TypeOf(object)

	for i := 0; i < ift.NumField(); i++ {
		v := ifv.Field(i)

		switch v.Kind() {
		case reflect.Struct:
			fields = append(fields, DeepFields(v.Interface())...)
		default:
			name := getCSVTypeName(ift.Field(i))
			if name != "-" {
				fields = append(fields, valueWithName{Value: v, name: getCSVTypeName(ift.Field(i))})
			}
		}
	}

	return fields
}

func GetValues(object interface{}) map[string]string {
	result := make(map[string]string)
	if object == nil {
		return result
	}

	val := reflect.ValueOf(object)
	if val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		val = val.Elem()
	}

	for _, field := range DeepFields(object) {
		if field.Kind() == reflect.Map {
			for _, key := range field.MapKeys() {
				mapVal := field.MapIndex(key)
				result[key.String()] = fmt.Sprintf("%+v", mapVal.Interface())
			}
		} else {
			if !field.CanInterface() {
				continue
			}
			result[field.name] = fmt.Sprintf("%+v", field.Interface())
		}
	}

	return result
}

func ToCSV(arr []interface{}) ([]byte, error) {
	allMaps := []map[string]string{}

	for _, v := range arr {
		allMaps = append(allMaps, GetValues(v))
	}

	headers := map[string]struct{}{}

	for _, m := range allMaps {
		for key, _ := range m {
			headers[key] = struct{}{}
		}
	}

	headersArr := []string{}
	for k, _ := range headers {
		headersArr = append(headersArr, k)
	}
	sort.Strings(headersArr)

	result := [][]string{headersArr}
	for _, m := range allMaps {
		row := []string{}
		for _, header := range headersArr {
			row = append(row, m[header])
		}
		result = append(result, row)
	}

	resultBytes := []byte{}
	buf := bytes.NewBuffer(resultBytes)

	w := csv.NewWriter(buf)
	err := w.WriteAll(result)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
