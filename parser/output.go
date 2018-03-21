package parser

import (
	"fmt"
	"os"

	"sort"

	"github.com/apanichkina/KSAMSimpleMathModel/csv"
	"github.com/gocarina/gocsv"
)

func PrintToCsv(filename string, output interface{}) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open %q to write: %s", filename, err)
	}
	defer f.Close()

	err = gocsv.MarshalFile(output, f) // Use this to save the CSV back to the file
	if err != nil {
		return fmt.Errorf("can't write to %q csv: %s", filename, err)
	}
	return nil
}

func PrintToFile(filename string, output []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open %q to write: %s", filename, err)
	}
	defer f.Close()

	_, err = f.Write(output)
	if err != nil {
		return fmt.Errorf("can't write to %q csv: %s", filename, err)
	}
	return nil
}

type mergeable interface {
	Prefix() string
	GetUniq() map[string]string
}

func mergeStructs(input ...mergeable) map[string]string {
	result := map[string]string{}
	for _, elem := range input {
		for k, v := range csv.GetValues(elem) {
			result[fmt.Sprintf("%s_%s", elem.Prefix(), k)] = v
		}
		for k, v := range elem.GetUniq() {
			result[k] = v
		}

	}
	return result
}

type mergeInputRequest struct {
	RequestResultInc
}

func (m mergeInputRequest) Prefix() string {
	return m.Transaction
}

func (m mergeInputRequest) GetUniq() map[string]string {
	return csv.GetValues(struct {
		IncrementValueMap `csv:"changed"`
		DatabaseValueMap  `csv:"model"`
	}{
		m.Increments,
		m.Queries,
	})
}

type mergedResult struct {
	Serial int
	Result map[string]string `csv:"_"`
}

func TransformBeforeOutput(input RequestsResultsInc) []interface{} {
	inputBySerial := map[int][]mergeable{}
	for _, v := range input {
		if _, ok := inputBySerial[v.SerialNumber]; !ok {
			inputBySerial[v.SerialNumber] = []mergeable{}
		}
		inputBySerial[v.SerialNumber] = append(inputBySerial[v.SerialNumber], mergeInputRequest{v})
	}

	fmt.Println(inputBySerial)

	resultForSort := []mergedResult{}

	for k, values := range inputBySerial {
		resultForSort = append(resultForSort, mergedResult{
			Serial: k,
			Result: mergeStructs(values...),
		})
	}

	sort.Slice(resultForSort, func(i, j int) bool {
		return resultForSort[i].Serial < resultForSort[j].Serial
	})

	result := []interface{}{}
	for _, v := range resultForSort {
		result = append(result, v)
	}

	return result
}
