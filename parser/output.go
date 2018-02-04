package parser

import (
	"fmt"
)

type QueriesMinTime struct {
	Query *Query  `csv:"query"`
	Time  float64 `csv:"time"`
}

// Convert the internal date as CSV string
func (q *Query) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", q.Name), nil
}

func (a QueriesMinTime) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %.2f}", a.Query.GetID(), a.Time)
}

type QueriesMinTimes []QueriesMinTime

type CSVData struct {
	Header string
	Data   []string
}
