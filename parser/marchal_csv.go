package parser

import "fmt"

//////// NOT USED ///////
func (f FullFloat64) MarshalCSV() (string, error) {
	return fmt.Sprintf("%f", f), nil
}

//////// NOT USED ///////

// Convert the internal date as CSV string
func (q *Query) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", q.Name), nil
}

func (t *Transaction) MarshalCSV() (string, error) {
	return fmt.Sprintf("%s", t.Name), nil
}
