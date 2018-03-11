package parser

import (
	"fmt"
	"strings"
)

// INPUT

func (a *TableInQuery) String() string {
	return fmt.Sprintf("{%s}", a.Pseudoname)
}

func (a *Increment) String() string {
	return fmt.Sprintf("{%s, %s, %f, %f, %f, %d}", a.ObjId, a.FieldName, a.From, a.Step, a.To, a.StepsCount)
}

func (arr TableIDs) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.GetID())
	}

	return strings.Join(result, ",")
}

// OUTPUT

func (arr TableNames) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.Table.Name)
	}

	return strings.Join(result, ",")
}

func (a QueriesMinTime) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %f, %f, %f, %f, %f}", a.Query.Name, a.Time, a.TimeIO, a.OrderTime, a.RowsCount, a.RowSize)
}

func (a TransactionResult) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %f, %f, %f}", a.Transaction, a.Time, a.DiscCharge, a.ProcCharge)
}
