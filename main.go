package main

import (
	"flag"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"github.com/apanichkina/KSAMSimpleMathModel/helper"
)

var fileInput = flag.String("in", "./data/true_input.json", "in - input model file")

func main() {
	flag.Parse()

	// парсинг входного json
	inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	helper.CheckError("parse error. ", err)

	result, err := math.Evaluate(inputparams)
	helper.CheckError("math core error. ", err)

	// генерация csv
	err = parser.PrintToCsv("data/result.csv", result)
	helper.CheckError("", err)
}
