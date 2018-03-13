package math

import (
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"reflect"
)

var GLOBALVARS parser.GlobalVariables
var TEST bool
var TESTSEQUENCE []int
var TESTQUERYNAME string

func Evaluate(inputParams parser.InputParams, globalVariables parser.GlobalVariables) (parser.RequestsResultsInc, error) {
	fmt.Println("VERSION: ", 1.1)
	//var output = parser.Errors{parser.Error{Message: "test"}}
	GLOBALVARS = globalVariables
	TEST = false
	TESTSEQUENCE = []int{0, 1, 2, 3, 4, 5}
	TESTQUERYNAME = "Q91"
	resultByRequest, err := Increment(inputParams)
	// resultByRequest, err := EvaluateRequest(inputParams)
	if err != nil {
		return nil, err
	}

	return resultByRequest, nil // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

func Increment(inputParams parser.InputParams) (parser.RequestsResultsInc, error) {
	fmt.Println("%v", inputParams.IncrementMap)
	//var index
	//var indexVal
	var incrementCount = len(inputParams.Increment)
	fmt.Println(reflect.TypeOf(incrementCount))
	// Инициализация первичными значениями
	var IncrementValues = make(map[string]interface{})
	for i, val := range inputParams.Increment {
		if len(val.PosibleValues) != 0 {
			IncrementValues[getIncrementID(val.ObjId, val.FieldName)] = val.PosibleValues[0]
		} else {
			IncrementValues[getIncrementID(val.ObjId, val.FieldName)] =  val.From
		}
		fmt.Println(i, " ", val, val.PosibleValues)
	}
	fmt.Println(IncrementValues)
	// Проход по всем значениям
	Incr(0, inputParams.Increment, IncrementValues)
	fmt.Println(IncrementValues)

	resultByRequest, err := EvaluateRequest(inputParams)
	if err != nil {
		return nil, err
	}


	var final parser.RequestsResultsInc
	//fmt.Println(reflect.TypeOf(resultByRequest), resultByRequest[0] ) // resultByRequest
	for _, val := range resultByRequest {
		var resultIncremented = parser.RequestResultInc{RequestResult: val, Increments: map[string]interface{}{"a": 0}}
		final = append(final, resultIncremented)
	}

	fmt.Println(final)

	return final, nil
}

func Incr(ind int, increments []*parser.Increment, IncrementValues map[string]interface{}) bool {
	if ind >= len(increments) {
		return false
	}
	var obj = increments[ind]
	var length = len(obj.PosibleValues)
	if length != 0 {
		IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = obj.PosibleValues[length - 1]
	} else {
		IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] =  obj.To
	}
	return Incr(ind+1, increments, IncrementValues)
}

func getIncrementID(nodeID string, fieldID string) string {
	return fmt.Sprint(nodeID, "_", fieldID)
}
