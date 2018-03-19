package math

import (
	"fmt"
	"reflect"

	"github.com/apanichkina/KSAMSimpleMathModel/csv"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

var GLOBALVARS parser.GlobalVariables
var TEST bool
var TESTSEQUENCE []int
var TESTQUERYNAME string

func Evaluate(inputParams parser.InputParams, globalVariables parser.GlobalVariables) ([]byte, error) {
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

	var result []interface{}
	for _, v := range resultByRequest {
		result = append(result, v)
	}

	return csv.ToCSV(result) // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

func Increment(inputParams parser.InputParams) (parser.RequestsResultsInc, error) {
	// fmt.Println("%v", inputParams.IncrementMap)
	//var index
	//var indexVal
	var incrementCount = len(inputParams.Increment)
	fmt.Println(reflect.TypeOf(incrementCount))
	// Инициализация первичными значениями
	var IncrementValues = make(map[string]interface{})
	var sn = 0
	var incrIndex = make([]int, len(inputParams.Increment))
	for i, _ := range inputParams.Increment {
		incrIndex[i] = 0
		//if len(val.PosibleValues) != 0 {
		//	IncrementValues[getIncrementID(val.ObjId, val.FieldName)] = val.PosibleValues[0]
		//} else {
		//	IncrementValues[getIncrementID(val.ObjId, val.FieldName)] = val.From
		//}
	}
	fmt.Println(IncrementValues, incrIndex)
	// Проход по всем значениям
	var final parser.RequestsResultsInc

	//var Inc = make(map[string]interface{})
	//for k, v := range IncrementValues {
	//	Inc[k] = v
	//}
	//
	//var resultByRequest, err = EvaluateRequest(inputParams, Inc, sn)
	//if err != nil {
	//	return nil, err
	//}
	//
	//final = append(final, resultByRequest...)

// second
	for Incr(0, inputParams.Increment, IncrementValues, incrIndex) {
		fmt.Println(IncrementValues, incrIndex)

		var Inc = make(map[string]interface{})
		for k, v := range IncrementValues {
			Inc[k] = v
		}

		var resultByRequest, err = EvaluateRequest(inputParams, Inc, sn)
		if err != nil {
			return nil, err
		}

		final = append(final, resultByRequest...)
		sn += 1
	}

	// final = append(final, parser.RequestResultInc{}) разделитель


	fmt.Println(final)

	return final, nil
}

func Incr(ind int, increments []*parser.Increment, IncrementValues parser.IncrementValueMap, incrIndex []int) bool {

	if ind >= len(increments) {
		incrIndex[0] += 1
		return true
	}



	var obj = increments[ind]
	var length = len(obj.PosibleValues)

	var curInd = incrIndex[ind]
	if length != 0 {
		if curInd < length {
			IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = obj.PosibleValues[curInd]
		} else {
			if ind == len(increments) - 1 {
				return false
			} else {
				incrIndex[ind] = 0
				incrIndex[ind+1] += 1
				return Incr(ind, increments, IncrementValues, incrIndex)
			}
		}

	} else {
		var newVal = obj.From + obj.Step * float64(curInd)
		if newVal <= obj.To {
			IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = newVal

		} else {
			if ind == len(increments) - 1 {
				return false
			} else {
				incrIndex[ind] = 0
				incrIndex[ind+1] += 1
				return Incr(ind, increments, IncrementValues, incrIndex)
			}
		}
	}

	return Incr(ind+1, increments, IncrementValues, incrIndex)












	//
	//
	//if ind >= len(increments) {
	//	return false
	//}
	//var obj = increments[ind]
	//var length = len(obj.PosibleValues)
	//
	//var curInd = incrIndex[ind]
	//if length != 0 {
	//	if curInd < length {
	//		IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = obj.PosibleValues[curInd]
	//	}
	//} else {
	//	var newVal = obj.From + obj.Step * float64(curInd)
	//	if newVal <= obj.To {
	//		IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = newVal
	//	}
	//}
	//
	//var newInd = incrIndex[ind] + 1
	//if length != 0 {
	//	if newInd < length {
	//		incrIndex[ind] = newInd
	//	} else {
	//		incrIndex[ind] = 0
	//		return Incr(ind+1, increments, IncrementValues, incrIndex)
	//	}
	//} else {
	//	var newVal = obj.From + obj.Step * float64(newInd)
	//	if newVal > obj.To {
	//		IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = newVal
	//		incrIndex[ind] = newInd
	//	} else {
	//		incrIndex[ind] = 0
	//		return Incr(ind+1, increments, IncrementValues, incrIndex)
	//	}
	//}
	//return true
}

func getIncrementID(nodeID string, fieldID string) string {
	return fmt.Sprint(nodeID, "_", fieldID)
}
