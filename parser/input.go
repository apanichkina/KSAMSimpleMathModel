package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

func (t *Table) UnmarshalJSON(data []byte) error {
	type alias Table
	var temp alias

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("can't unmarshal to %T: %s", t, err)
	}

	if temp.L == 0 {
		temp.L = 200
	}
	*t = Table(temp)
	return nil
}

func (a *Attribute) UnmarshalJSON(data []byte) error {
	type alias Attribute
	var temp alias

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("can't unmarshal to %T: %s", a, err)
	}

	if temp.I == 0 {
		temp.I = 200
	}
	*a = Attribute(temp)
	return nil
}

var findNumsInQuotes = regexp.MustCompile(`"([-+]?)\d*\.\d+"|"\d+"`)
var findEmptyStrings = regexp.MustCompile(`(\s)*,*(\s)*("[^"]*")([:])(\s)*("")(\s)*,*(\s)*`)
var findEndOrStartCommas = regexp.MustCompile(`{(\s)*,|,(\s)*}`)

func prepareNumsInQuotes(input []byte) []byte {
	return []byte(findNumsInQuotes.ReplaceAllStringFunc(string(input), func(input string) string {
		if input == `""` {
			return "0"
		}
		return strings.Replace(input, `"`, ``, -1)
	}))
}

func prepareEmptyQuotes(input []byte) []byte {
	return []byte(findEmptyStrings.ReplaceAllStringFunc(string(input), func(input string) string {
		workingStr := strings.TrimSpace(input)
		hasPreComma := strings.HasPrefix(workingStr, ",")
		hasPostComma := strings.HasSuffix(workingStr, ",")
		switch {
		case hasPreComma && hasPostComma: // ..., "a": "",... -> ..., ...
			return ", "
		default:
			return ""
		}
	}))
}

func prepareEndStartCommas(input []byte) []byte {
	return []byte(findEndOrStartCommas.ReplaceAllStringFunc(string(input), func(input string) string {
		if strings.Contains(input, "{") {
			return "{"
		}
		return "}"
	}))
}

func PrepareJson(input []byte) []byte {
	return prepareEndStartCommas(
		prepareEmptyQuotes(
			prepareNumsInQuotes(input),
		),
	)
}

func GetInputParamsFromByteSlice(input []byte) (InputParams, error) {
	var result InputParams

	input = PrepareJson(input)

	err := json.Unmarshal(input, &result)
	if err != nil {
		return InputParams{}, fmt.Errorf("can't unmarshal: %s", err)
	}

	err = result.PrepareData()
	if err != nil {
		return InputParams{}, fmt.Errorf("can't prepare data: %s", err)
	}
	return result, nil
}

func GetInputParamsFromString(input string) (InputParams, error) {
	data := []byte(input)
	return GetInputParamsFromByteSlice(data)
}

func GetInputParamsFromFile(inputFile string) (InputParams, error) {
	raw, err := ioutil.ReadFile(inputFile)
	if err != nil {
		return InputParams{}, fmt.Errorf("can't get file from %q: %s", inputFile, err)
	}
	return GetInputParamsFromByteSlice(raw)
}

func (ip *InputParams) PrepareData() error {
	ip.setMaps()
	for _, p := range ip.DataModel {

		p.setMaps()

		for _, t := range p.Tables {
			err := t.setMaps()
			if err != nil {
				return err
			}
		}

		for _, q := range p.Queries {
			for _, tq := range q.TablesInQuery {
				err := tq.setPointers(*p)
				if err != nil {
					return err
				}
			}

			q.setMaps()
		}

		for _, t := range p.Transactions {
			err := t.setMaps()
			if err != nil {
				return err
			}
		}

	}

	err := ip.setPointers()
	if err != nil {
		return err
	}

	return nil
}
