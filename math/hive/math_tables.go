package hive

import (
	"fmt"
	"math"
)

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

func (j JoinTable) getMaxAttrV() float64 {
	var maxV float64 = 1

	for _, v := range j.Attributes {
		if v.V > maxV {
			maxV = v.V
		}
	}

	return maxV
}

func simpleJoinSelectivity(tables ...JoinTable) float64 {
	maxV := tables[0].getMaxAttrV()
	for _, v := range tables[1:] {
		maxV = math.Max(maxV, v.getMaxAttrV())
	}
	return 1 / maxV
}

func joinSelectivity(tables ...JoinTable) float64 {
	return simpleJoinSelectivity(tables...)
}

func Join(
	name string,
	tables ...JoinTable,
) *Table {
	tr := joinSelectivity(tables...)
	fmt.Println(tr)
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
	for _, a := range result.attrs {
		fmt.Println(a)
		if a.V > result.Tr {
			a.V = result.Tr
		}
	}

	return result
}

func Select(t Table, a ...*Attribute) *Table {
	return NewTable(t.Name, t.Tr, a...)
}
