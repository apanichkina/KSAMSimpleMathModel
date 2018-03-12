package hive

import "math"

func TableScan(t Table) Table {
	return t
}

type Projection struct {
	Table      *Table
	Attributes []*Attribute
}

type joinPresentation struct {
	joins []JoinTable
}

type JoinTable struct {
	Table      *Table
	Attributes []*Attribute
}

func (j JoinTable) getFirstAttrV() float64 {
	if len(j.Attributes) == 0 {
		return 1
	}

	return j.Attributes[0].V
}

func simpleJoinSelectivity(tables ...JoinTable) float64 {
	maxV := tables[0].getFirstAttrV()
	for _, v := range tables[1:] {
		maxV = math.Max(maxV, v.getFirstAttrV())
	}
	return 1 / maxV
}

func joinSelectivity(tables ...JoinTable) float64 {
	attrsCount := len(tables[0].Attributes)
	for _, v := range tables[1:] {
		if len(v.Attributes) != attrsCount {
			return simpleJoinSelectivity(tables...)
		}
	}

	var resultV float64 = 1
	for i := 0; i < attrsCount; i++ {
		maxV := tables[0].Attributes[i].V
		for _, v := range tables[1:] {
			maxV = math.Max(maxV, v.Attributes[i].V)
		}

		resultV *= maxV
	}

	return 1 / resultV
}

func Join(
	name string,
	tables ...JoinTable,
) *Table {
	tr := joinSelectivity(tables...)
	resultAttributes := []*Attribute{}

	for _, t := range tables {
		tr *= t.Table.Tr
		resultAttributes = append(resultAttributes, t.Attributes...)
	}

	return NewTable(name, tr, resultAttributes...)
}

func Filter(t Table, selectivity float64) Table {
	result := t
	result.Tr *= selectivity
	return t
}

func Select(t Table, a ...*Attribute) *Table {
	return NewTable(t.Name, t.Tr, a...)
}
