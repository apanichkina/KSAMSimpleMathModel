package hive

import "fmt"

// Table represents Table inside query
type Table struct {
	ID string

	Name string  // name of table in query
	Tr   float64 // number of rows

	attrs   []*Attribute
	TszTemp *float64
}

// Attribute represents attribute of the table with estimate size in bytes
type Attribute struct {
	Size float64
	V    float64

	parent *Table

	projection bool
	inJoin     bool
	filter     float64
}

// NewTable constructor for Table struct
func NewTable(name string, tr float64, attrs ...*Attribute) *Table {
	t := Table{
		Name: name,
		Tr:   tr,
	}

	for _, a := range attrs {
		a.parent = &t
		t.attrs = append(t.attrs, a)
	}

	return &t
}

func (t Table) Tsz() float64 {
	if t.TszTemp != nil {
		return *t.TszTemp
	}

	var tsz float64

	for _, a := range t.attrs {
		tsz += a.Size
	}

	//fmt.Printf("row size: %.0f\n", tsz)
	return tsz
}

// Cost describes CPU and IO Disk cost in nanoseconds
type Cost struct {
	fullcpu float64
	fullio  float64
	fullnet float64
	CPU     float64
	IO      float64
	NET     float64
}

// NewCost - constructor for Cost struct
func NewCost(cpu, io, net, workersCount float64) Cost {
	return Cost{
		fullcpu: cpu,
		fullio:  io,
		fullnet: net,
		CPU:     cpu / workersCount,
		IO:      io / workersCount,
		NET:     net / workersCount,
	}
}

// Add returns new Cost object with sum of CPU's and IO's
func (c Cost) Add(val Cost) Cost {
	return Cost{
		fullio:  c.fullio + val.fullio,
		fullcpu: c.fullcpu + val.fullcpu,
		fullnet: c.fullnet + val.fullnet,
		CPU:     c.CPU + val.CPU,
		IO:      c.IO + val.IO,
		NET:     c.NET + val.NET,
	}
}

func SumCosts(costs ...Cost) Cost {
	var result Cost
	for _, c := range costs {
		result = result.Add(c)
	}
	return result
}

func (c Cost) Seconds() float64 {
	return (c.CPU + c.IO + c.NET) / 1e+9
}

func (c Cost) String() string {
	return fmt.Sprintf("%.2fs, fullcpu: %.2fs, fullio: %.2fs, fullnet: %.2fs", c.Seconds(), c.fullcpu/1e+9, c.fullio/1e+9, c.fullnet/1e+9)
}
