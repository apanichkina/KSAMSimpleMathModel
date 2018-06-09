package hive

import (
	"fmt"

	"context"

	"strings"

	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

func Evaluate(ctx context.Context, inputParams parser.InputParams, extra interface{}) ([]byte, error) {
	result := []string{}
	//fmt.Println("evaluating")
	for _, dataModel := range inputParams.DataModel {
		for _, q := range dataModel.Queries {
			//fmt.Println(q.Name)
			cost := EvaluateQueryPlan(*q)
			//fmt.Printf("[RESULT] %s,%s,%+v\n", dataModel.Name, q.Name, cost)
			result = append(result, fmt.Sprintf("%.0f", cost.Seconds()))
		}
	}
	return []byte(strings.Join(result, ",")), nil // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

type manager struct {
	query parser.Query

	memTables map[string]*Table
	memAttrs  map[string]*Attribute
}

func (m *manager) getAttributes(pseudoID string, t parser.Table) []*Attribute {
	result := []*Attribute{}
	for _, v := range t.Attributes {
		a := Attribute{
			Size:       v.Size,
			V:          v.I,
			filter:     m.getFilter(pseudoID, v.GetID()),
			projection: m.getProjection(pseudoID, v.GetID()),
			inJoin:     m.getJoin(pseudoID, v.GetID()),
		}
		m.memAttrs[v.GetID()] = &a
		result = append(result, &a)
	}
	return result
}

func (m *manager) getProjection(tableID, attrID string) bool {
	for _, v := range m.query.Projection {
		if v.TableId == tableID && v.AttributeId == attrID {
			return true
		}
	}
	return false
}

func (m *manager) getJoin(tableID, attrID string) bool {
	for _, j := range m.query.Joins {
		for _, attr := range j.Join {
			for _, a := range attr.Attributes {
				if attrID == a {
					return true
				}
			}
		}
	}
	return false
}

func (m *manager) getFilter(tableID, attrID string) float64 {
	for _, v := range m.query.Conditions {
		if v.TableId == tableID && v.AttributeId == attrID {
			return v.P
		}
	}
	return 0
}

func (m *manager) getTables() []*Table {
	tables := []*Table{}
	for _, v := range m.query.TablesInQuery {
		t := NewTable(v.Pseudoname, v.Table.T, m.getAttributes(v.ID.ID, *v.Table)...)
		m.memTables[v.TableId] = t
		tables = append(tables, t)
	}
	return tables
}

func (m *manager) getJoins() []joinPresentation {
	joins := []joinPresentation{}

	for _, v := range m.query.Joins {
		tables := []JoinTable{}
		for _, j := range v.Join {
			attrs := []*Attribute{}

			for _, a := range j.Attributes {
				attrs = append(attrs, m.memAttrs[a])
			}

			tables = append(tables, JoinTable{
				Table:      m.memTables[m.query.TablesInQueryMap[j.TableId].Table.GetID()],
				Attributes: attrs,
			})
		}

		joins = append(joins, joinPresentation{tables})
	}

	return joins
}

func newManager(query parser.Query) manager {
	return manager{
		query: query,

		memTables: map[string]*Table{},
		memAttrs:  map[string]*Attribute{},
	}
}

func prepareInputQuery(query parser.Query) ([]*Table, []joinPresentation) {
	manager := newManager(query)

	return manager.getTables(), manager.getJoins()
}

func EvaluateQueryPlan(query parser.Query) Cost {
	tables, joins := prepareInputQuery(query)

	var finalCost Cost
	for i, t := range tables {
		//fmt.Printf("%+v\n", t)

		mappers := t.Tr * t.Tsz() / BlockSize

		tableScan := TableScanCost(*t, mappers)
		filterCost := FilterCost(*t, mappers)

		//fmt.Printf("tableScan: %+v\n", tableScan)

		finalCost = finalCost.Add(tableScan)

		hasFilter := false

		// Filter
		for _, a := range t.attrs {
			//	fmt.Printf("%+v\n", a)
			if a.filter != 0 {
				hasFilter = true
				*t = Filter(*t, a.filter)
			}
		}
		if hasFilter {
			//fmt.Printf("filterScan: %+v\n", filterCost)
			finalCost = finalCost.Add(filterCost)
		}

		var selectedAttrs []*Attribute
		// Select
		for _, a := range t.attrs {
			//fmt.Println(a)
			if a.projection || a.inJoin {
				selectedAttrs = append(selectedAttrs, a)
			}
		}
		*tables[i] = *Select(*t, selectedAttrs...)
	}

	//fmt.Println("final cost", finalCost)

	for _, t := range tables {
		for _, a := range t.attrs {

			_ = a
			//fmt.Println(a)
		}
	}

	var joinsCost Cost

	processedTables := map[*Table]struct{}{}

	var result *Table
	for _, j := range joins {
		newT := Join("j", j.joins...)
		joinTables := make([]*Table, len(j.joins))
		if len(processedTables) == 0 {
			for i, v := range j.joins {
				if result == nil {
					result = v.Table
				}
				joinTables[i] = v.Table
			}
		}

		//fmt.Println(newT)
		joinsCost = joinsCost.Add(CommonJoinCost(newT.Tr, joinTables...))
		//fmt.Printf("join cost: %+v---%.2fs\n", joinsCost, joinsCost.Seconds())
	}
	return finalCost.Add(joinsCost)
}
