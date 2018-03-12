package hive

// Table represents Table inside query
type Table struct {
	Name string  // name of table in query
	Tr   float64 // number of rows

	attrs []*Attribute
}

// Attribute represents attribute of the table with estimate size in bytes
type Attribute struct {
	Size float64
	V    float64

	parent *Table

	projection bool
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
	var tsz float64

	for _, a := range t.attrs {
		tsz += a.Size
	}

	return tsz
}

// Cost describes CPU and IO Disk cost in nanoseconds
type Cost struct {
	CPU float64
	IO  float64
}

// NewCost - constructor for Cost struct
func NewCost(cpu, io float64) Cost {
	return Cost{
		CPU: cpu,
		IO:  io,
	}
}

// Add returns new Cost object with sum of CPU's and IO's
func (c Cost) Add(val Cost) Cost {
	return Cost{
		CPU: c.CPU + val.CPU,
		IO:  c.IO + val.IO,
	}
}
