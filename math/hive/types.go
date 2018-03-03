package hive

// Table represents Table inside query
type Table struct {
	Name string  // name of table in query
	Tr   float64 // number of rows
	Tsz  float64 // average row size in bytes
}

func NewTable(name string, tr, tsz float64) Table {
	return Table{
		Name: name,
		Tr:   tr,
		Tsz:  tsz,
	}
}

type Cost struct {
	CPU float64
	IO  float64
}

func NewCost(cpu, io float64) Cost {
	return Cost{
		CPU: cpu,
		IO:  io,
	}
}
