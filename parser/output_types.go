package parser

//////// NOT USED ///////
type FullFloat64 float64

type Error struct {
	Message string `csv:"error_message"`
}

type Errors []Error

//////// NOT USED ///////

type QueriesMinTime struct {
	Query     *Query  `csv:"-"`
	Time      float64 `csv:"time"`
	TimeIO    float64 `csv:"-"`
	OrderTime float64 `csv:"-"`
	RowsCount float64 `csv:"-"`
	RowSize   float64 `csv:"-"`
}

type TransactionResult struct {
	Transaction string  `csv:"-"`
	Time        float64 `csv:"time"`
	DiscCharge  float64 `csv:"disc-p"`
	ProcCharge  float64 `csv:"proc-p"`
	Size        float64 `csv:"size"`
}

type RequestResult struct {
	TransactionResult
	NetworkCharge float64 `csv:"net-p"`
	NetworkTime   float64 `csv:"-"`
	NetworkSpeed  float64 `csv:"-"`
}

type RequestResultInc struct {
	SerialNumber int `csv:"-"` // выводится уникально
	RequestResult
	Increments IncrementValueMap `csv:"-"` // выводится уникально
	Queries    DatabaseValueMap  `csv:"-"` // выводится уникально
}

type IncrementValueMap map[string]interface{}
type DatabaseValueMap map[string]QueryValueMap
type QueryValueMap map[string]QueriesMinTime

type RequestsResults []RequestResult

type RequestsResultsInc []RequestResultInc

type TransactionsResults []TransactionResult

type CSVData struct {
	TransactionsResults
	QueriesMinTimes
}

type QueriesMinTimes []QueriesMinTime

func (q QueriesMinTimes) ToQueryValueMap() QueryValueMap {
	var result QueryValueMap = map[string]QueriesMinTime{}
	for _, v := range q {
		result[v.Query.Name] = v
	}
	return result
}
