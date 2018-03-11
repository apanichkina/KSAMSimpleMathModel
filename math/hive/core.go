package hive

import "github.com/apanichkina/KSAMSimpleMathModel/parser"

func Evaluate(inputParams parser.InputParams, extra interface{}) ([]byte, error) {
	//var output = parser.Errors{parser.Error{Message: "test"}}
	q := inputParams.DataModel[0].Queries[0]
	resultByRequest, err := EvaluateRequest(inputParams)
	if err != nil {
		return nil, err
	}

	return resultByRequest, nil // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

func prepareInputQuery(query parser.Query) {
	tables := []Table{}
	for _, v := range query.TablesInQuery {
		tables = append(tables, NewTable(v.Table.Name, v.Table.Size))
	}
}

func evaluateQueryPlan(query parser.Query) {
	query.Joins[0].Join[0].Attributes
}
