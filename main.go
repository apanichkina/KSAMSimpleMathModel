package main

import (
	"flag"
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"log"
)

func checkError(message string, err error) {
	if err != nil {
		var fullError = parser.Errors{{Message: fmt.Sprint(message, err)}}
		var err1 = parser.PrintToCsv("data/result.csv", fullError)
		if err1 != nil {
			log.Fatal(message, err1)
		}
		log.Fatal(message, err)
	}
}

var fileInput = flag.String("in", "./data/true_input.json", "in - input model file")

func main() {
	flag.Parse()

	// парсинг входного json
	inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	checkError("parse error", err)

	result, err := math.Evaluate(inputparams)
	checkError("math core error", err)

	// генерация csv
	err = parser.PrintToCsv("data/result.csv", result)
	checkError("", err)
}
