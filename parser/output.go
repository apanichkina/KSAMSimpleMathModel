package parser

import (
	"fmt"
	"os"
	"github.com/gocarina/gocsv"
)

type FullFloat64 float64

type Error struct {
	Message string  `csv:"error_message"`
}

type Errors []Error

type QueriesMinTime struct {
	Query *Query  `csv:"query"`
	Time  float64 `csv:"time"`
	TimeIO float64 `csv:"timeIO"`
	OrderTime float64 `csv:"Ordertime"`
	RowsCount float64 `csv:"RowsCount"`
	RowSize float64 `csv:"RowsSize"`
}

// Convert the internal date as CSV string
func (q *Query) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", q.Name), nil
}

func (t *Transaction) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", t.Name), nil
}

func (f FullFloat64) MarshalCSV() (string, error) {
	return fmt.Sprintf("%f", f), nil
}

func (a QueriesMinTime) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %f, %f, %f, %f, %f}", a.Query.Name, a.Time, a.TimeIO, a.OrderTime, a.RowsCount, a.RowSize)
}

type QueriesMinTimes []QueriesMinTime

func (a TransactionResult) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %f, %f, %f}", a.Transaction, a.Time, a.DiscCharge, a.ProcCharge)
}

func (a *TableInQuery) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s}", a.Pseudoname)
}

func (a *Increment) String() string { // правило печати объектов типа QueriesMinTime
	return fmt.Sprintf("{%s, %s, %f, %f, %f, %d}", a.ObjId, a.FieldName, a.From, a.Step, a.To, a.StepsCount)
}

type TransactionResult struct {
	Transaction *Transaction  `csv:"transaction"`
	Time  float64 `csv:"time"`
	DiscCharge float64 `csv:"disc-p"`
	ProcCharge float64 `csv:"proc-p"`
	Size float64 `csv:"size-byte"`
}

type RequestResult struct {
	TransactionResult
	NetworkCharge float64 `csv:"net-p"`
	NetworkTime float64 `csv:"net-M"`
	NetworkSpeed float64 `csv:"net-speed"`
}

type RequestsResults []RequestResult

type TransactionsResults []TransactionResult

type CSVData struct {
	TransactionsResults
	QueriesMinTimes
}

func PrintToCsv(filename string, output interface{}) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open %q to write: %s", filename, err)
	}
	defer f.Close()

	err = gocsv.MarshalFile(output, f) // Use this to save the CSV back to the file
	if err != nil {
		return fmt.Errorf("can't write to %q csv: %s", filename, err)
	}
	return nil
}