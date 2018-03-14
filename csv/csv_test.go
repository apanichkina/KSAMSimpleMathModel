package csv

import (
	"reflect"
	"testing"
)

type Test struct {
	Test   string
	Inside map[string]interface{}
}

func TestGetHeaders(t *testing.T) {
	for _, v := range []struct {
		input  interface{}
		output map[string]string
	}{
		{nil, map[string]string{}},
		{struct {
			A string
			b string
		}{
			"a value",
			"b value",
		}, map[string]string{"A": "a value"}},
		{struct {
			A string
			Test
		}{
			"hello",
			Test{"test", map[string]interface{}{"C": 1, "B": "str"}},
		}, map[string]string{"Test": "test", "A": "hello", "C": "1", "B": "str"}},
	} {
		o := GetValues(v.input)
		if !reflect.DeepEqual(o, v.output) {
			t.Errorf("expected: %q, got: %q", v.output, o)
		}
	}
}

func TestGetCSV(t *testing.T) {
	for _, v := range []struct {
		input  interface{}
		output map[string]string
	}{
		{nil, map[string]string{}},
		{struct {
			A string
			b string
		}{
			"a value",
			"b value",
		}, map[string]string{"A": "a value"}},
		{struct {
			A string
			Test
		}{
			"hello",
			Test{"test", map[string]interface{}{"C": 1, "B": "str"}},
		}, map[string]string{"Test": "test", "A": "hello", "C": "1", "B": "str"}},
	} {
		o, err := ToCSV([]interface{}{v.input})
		if err != nil {
			t.Errorf(err.Error())
		}
		if !reflect.DeepEqual(o, v.output) {
			t.Errorf("expected: %q, got: %q", v.output, o)
		}
	}
}
