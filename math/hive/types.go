package hive

// Table represents Table inside query
type Table struct {
	Name string  // name of table in query
	Tr   float64 // number of rows
	Tsz  float64 // average row size in bytes
}
