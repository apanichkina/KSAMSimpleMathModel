package main

import (
	"flag"

	"context"

	"github.com/apanichkina/KSAMSimpleMathModel/helper"
	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

var fileInput = flag.String("in", "./math/hive_runner/data/asd.json", "in - input model file")

func main() {
	flag.Parse()

	// парсинг входного json
	inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	helper.CheckError("parse error. ", err)

	var globalVariables = parser.GlobalVariables{D: 18432, D_ind: 16384, K: 4}
	result, err := hive.Evaluate(context.Background(), inputparams, globalVariables)
	helper.CheckError("math core error. ", err)

	// генерация csv
	err = parser.PrintToFile("data/result.csv", result)
	helper.CheckError("", err)
}
