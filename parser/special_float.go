package parser

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

var r = strings.NewReplacer("\"", "", "\\", "", "\\`", "")

type Float64 float64

func (u *Float64) UnmarshalJSON(data []byte) error {

	var a float64
	err := json.Unmarshal([]byte(r.Replace(string(data))), &a)
	if err != nil {
		return fmt.Errorf("can't unmarshal %q to SuperFloat64: %s", data, err)
	}

	*u = Float64(a)
	return nil
}

func (f Float64) F64() float64 {
	return float64(f)
}

func Min(a, b Float64) Float64 {
	return Float64(math.Min(float64(a), float64(b)))
}

func Max(a, b Float64) Float64 {
	return Float64(math.Max(float64(a), float64(b)))
}
