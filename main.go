package main

import (
	"flag"

	"github.com/apanichkina/KSAMSimpleMathModel/helper"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"context"
)

var fileInput = flag.String("in", "./data/popenkov.json", "in - input model file")

func main() {
	flag.Parse()

	// парсинг входного json
	inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	helper.CheckError("parse error. ", err)

	var globalVariables = parser.GlobalVariables{D: 18432, D_ind: 16384, K: 4}
	result, err := math.Evaluate(context.Background(), inputparams, globalVariables)
	helper.CheckError("math core error. ", err)

	// генерация csv
	err = parser.PrintToFile("data/result.csv", result)
	helper.CheckError("", err)
}
