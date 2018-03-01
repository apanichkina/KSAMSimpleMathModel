package parser

import "testing"

func TestPrepareJson(t *testing.T) {
	for _, v := range []struct {
		input, output string
	}{
		{`{"a":"a"}`, `{"a":"a"}`},
		{`{"a":"1"}`, `{"a":1}`},
		{`{"a":"a1"}`, `{"a":"a1"}`},
	} {
		o := string(PrepareJson([]byte(v.input)))
		if o != v.output {
			t.Errorf("expected: %q, got: %q", v.output, o)
		}
	}
}
