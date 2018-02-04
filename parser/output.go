package parser

import (
	"fmt"
)

type QueriesMinTime struct {
	Query *Query
	Time float64
}

func (a QueriesMinTime) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %.2f}", a.Query.GetID(), a.Time,)
}
type QueriesMinTimes []QueriesMinTime


type CSVData struct {
	Header string
	Data []string
}