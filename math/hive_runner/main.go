package main

import (
	"flag"

	"context"

	"bufio"
	"os"

	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/helper"
	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

var fileInput = flag.String("in", "./math/hive_runner/data/hive_test_0.9.txt", "in - input model file")

func main() {
	flag.Parse()

	f, err := os.Open(*fileInput)
	helper.CheckError("file error. ", err)

	scanner := bufio.NewScanner(f)

	results := [][]byte{}

	//for {
	//
	//	line, _, err := scanner.ReadLine()
	//
	//	fmt.Println(string(line))
	//
	//	inputparams, err := parser.GetInputParamsFromString(string(line))
	//	helper.CheckError("parse error. ", err)
	//
	//	result, err := hive.Evaluate(context.Background(), inputparams, nil)
	//	helper.CheckError("math core error. ", err)
	//
	//	results = append(results, result)
	//	if err != nil {
	//		break
	//	}
	//}

	for scanner.Scan() {
		fmt.Println("got model")
		inputparams, err := parser.GetInputParamsFromString(scanner.Text())
		helper.CheckError("parse error. ", err)

		result, err := hive.Evaluate(context.Background(), inputparams, nil)
		helper.CheckError("math core error. ", err)

		results = append(results, result)
	}
	err = scanner.Err()
	helper.CheckError("scanner error. ", err)

	for _, v := range results {
		fmt.Println(string(v))
	}

	//// парсинг входного json
	//inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	//helper.CheckError("parse error. ", err)
	//
	//result, err := hive.Evaluate(context.Background(), inputparams, nil)
	//helper.CheckError("math core error. ", err)

	// генерация csv
	//err = parser.PrintToFile("./math/hive_runner/data/result.csv", results)
	//helper.CheckError("", err)
}
