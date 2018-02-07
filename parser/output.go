package parser

import (
	"fmt"
)

type FullFloat64 float64

type Error struct {
	Message string  `csv:"error_message"`
}

type Errors []Error

type QueriesMinTime struct {
	Query *Query  `csv:"query"`
	Time  FullFloat64 `csv:"time"`
}

// Convert the internal date as CSV string
func (q *Query) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", q.Name), nil
}

func (f FullFloat64) MarshalCSV() (string, error) {
	return fmt.Sprintf("%f", f), nil
}

func (a QueriesMinTime) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %f}", a.Query.GetID(), a.Time)
}

type QueriesMinTimes []QueriesMinTime

type CSVData struct {
	Error
	QueriesMinTime
}
