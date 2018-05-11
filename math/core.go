package math

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/csv"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"context"
)

var GLOBALVARS parser.GlobalVariables
var TEST bool
var TESTSEQUENCE []int
var TESTQUERYNAME string
var ALREADY_CALCULATED_DATA_MODEL QueryTimesCache

func Evaluate(ctx context.Context, inputParams parser.InputParams, globalVariables parser.GlobalVariables) ([]byte, error) {
	fmt.Println("VERSION: ", 1.5)
	//var output = parser.Errors{parser.Error{Message: "test"}}
	GLOBALVARS = globalVariables
	TEST = false
	TESTSEQUENCE = []int{0, 1, 2, 3, 4, 5}
	TESTQUERYNAME = "Q91"
	ALREADY_CALCULATED_DATA_MODEL = make(QueryTimesCache)
	resultByRequest, err := Increment(ctx, inputParams)
	// resultByRequest, err := EvaluateRequest(inputParams)
	if err != nil {
		return nil, err
	}

	result := parser.TransformBeforeOutput(resultByRequest)
	return csv.ToCSV(result) // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

func Increment(ctx context.Context, inputParams parser.InputParams) (parser.RequestsResultsInc, error) {
	var incrementCount = len(inputParams.Increment)
	var final parser.RequestsResultsInc
	var sn = 0
	var IncrementValues = make(map[string]interface{})
	if incrementCount == 0 {
		var resultByRequest, err = EvaluateRequest(inputParams, IncrementValues, sn)
		if err != nil {
			return nil, err
		}

		final = append(final, resultByRequest...)
		return final, nil
	}

	var incrIndex = make([]int, incrementCount)
	for i, _ := range inputParams.Increment {
		incrIndex[i] = 0
	}
	//fmt.Println(IncrementValues, incrIndex)
	// Проход по всем значениям

	for Incr(0, inputParams.Increment, IncrementValues, incrIndex) {
		// fmt.Println(IncrementValues, incrIndex)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

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
			if ind == len(increments)-1 {
				return false
			} else {
				incrIndex[ind] = 0
				incrIndex[ind+1] += 1
				return Incr(ind, increments, IncrementValues, incrIndex)
			}
		}

	} else {
		var newVal = obj.From + obj.Step*float64(curInd)
		if newVal <= obj.To {
			IncrementValues[getIncrementID(obj.ObjId, obj.FieldName)] = newVal

		} else {
			if ind == len(increments)-1 {
				return false
			} else {
				incrIndex[ind] = 0
				incrIndex[ind+1] += 1
				return Incr(ind, increments, IncrementValues, incrIndex)
			}
		}
	}

	return Incr(ind+1, increments, IncrementValues, incrIndex)
}

func getIncrementID(nodeID string, fieldID string) string {
	return fmt.Sprint(nodeID, "_", fieldID)
}
