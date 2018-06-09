package data

import (
	"fmt"

	"bufio"
	"os"

	"github.com/apanichkina/KSAMSimpleMathModel/helper"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

var fileInput09 = "./math/hive_runner/data/hive_test_0.9.txt"
var fileInput01 = "./math/hive_runner/data/hive_test_0.1.txt"

func read(filename string) []parser.Query {
	f, err := os.Open(filename)
	helper.CheckError("file error. ", err)

	scanner := bufio.NewScanner(f)

	results := []parser.Query{}

	for scanner.Scan() {
		fmt.Println("got model")
		inputparams, err := parser.GetInputParamsFromString(scanner.Text())
		helper.CheckError("parse error. ", err)

		results = append(results, *inputparams.DataModel[0].Queries[0])
	}

	err = scanner.Err()
	helper.CheckError("scanner error. ", err)

	return results
}

func GetExperiments() ([]parser.Query, []parser.Query) {
	return read(fileInput09), read(fileInput01)
}
