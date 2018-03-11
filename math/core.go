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

func Evaluate(inputParams parser.InputParams, globalVariables parser.GlobalVariables) (parser.RequestsResults, error) {
	//var output = parser.Errors{parser.Error{Message: "test"}}
	GLOBALVARS = globalVariables
	TEST = false
	TESTSEQUENCE = []int{0, 1, 2, 3, 4, 5}
	TESTQUERYNAME = "Q91"
	Increment(inputParams)
	resultByRequest, err := EvaluateRequest(inputParams)
	if err != nil {
		return nil, err
	}

	return resultByRequest, nil // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

func Increment(inputParams parser.InputParams) {
	fmt.Println("%v", inputParams.IncrementMap)
	//var index
	//var indexVal
	var incrementCount = len(inputParams.Increment)
	fmt.Println(reflect.TypeOf(incrementCount))
	// Инициализация первичными значениями
	var IncrementValues = make(map[string]interface{})
	for i, val := range inputParams.Increment {
		IncrementValues[getIncrementID(val.ObjId, val.FieldName)] = val.From
		fmt.Println(i, " ", val, val.PosibleValues)
	}
	fmt.Println(IncrementValues)
	// Проход по всем значениям
	Incr(0, inputParams.Increment, IncrementValues)
	fmt.Println(IncrementValues)

}

func Incr(ind int, increments []*parser.Increment, IncrementValues map[string]interface{}) bool {
	if ind >= len(increments) {
		return false
	}
	var value = increments[ind].To
	IncrementValues[getIncrementID(increments[ind].ObjId, increments[ind].FieldName)] = value
	return Incr(ind+1, increments, IncrementValues)
}

func getIncrementID(nodeID string, fieldID string) string {
	return fmt.Sprint(nodeID, "_", fieldID)
}
