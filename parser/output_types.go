package parser

//////// NOT USED ///////
type FullFloat64 float64

type Error struct {
	Message string `csv:"error_message"`
}

type Errors []Error

//////// NOT USED ///////

type QueriesMinTime struct {
	Query     *Query  `csv:"query"`
	Time      float64 `csv:"time"`
	TimeIO    float64 `csv:"timeIO"`
	OrderTime float64 `csv:"Ordertime"`
	RowsCount float64 `csv:"RowsCount"`
	RowSize   float64 `csv:"RowsSize"`
}

type TransactionResult struct {
	Transaction string `csv:"transaction_name"`
	Time        float64      `csv:"time"`
	DiscCharge  float64      `csv:"disc-p"`
	ProcCharge  float64      `csv:"proc-p"`
	Size        float64      `csv:"size-byte"`
}

type RequestResult struct {
	TransactionResult
	NetworkCharge float64 `csv:"net-p"`
	NetworkTime   float64 `csv:"net-M"`
	NetworkSpeed  float64 `csv:"net-speed"`
}

type RequestResultInc struct {
	RequestResult
	Increments map[string]interface{}
}

type RequestsResults []RequestResult

type RequestsResultsInc []RequestResultInc

type TransactionsResults []TransactionResult

type CSVData struct {
	TransactionsResults
	QueriesMinTimes
}

type QueriesMinTimes []QueriesMinTime
