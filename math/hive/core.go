package hive

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

func Evaluate(inputParams parser.InputParams, extra interface{}) ([]byte, error) {
	q := inputParams.DataModel[0].Queries[0]
	evaluateQueryPlan(*q)
	return nil, nil // []parser.CSVData{{TransactionsResults: resultByTransaction, QueriesMinTimes: resultByQuery}}, nil
}

type manager struct {
	query parser.Query

	memTables map[string]*Table
	memAttrs  map[string]*Attribute
}

func (m *manager) getAttributes(t parser.Table) []*Attribute {
	result := []*Attribute{}
	for _, v := range t.Attributes {
		a := Attribute{
			Size:       v.Size,
			V:          v.I,
			filter:     m.getFilter(t.GetID(), v.GetID()),
			projection: m.getProjection(t.GetID(), v.GetID()),
		}
		m.memAttrs[v.GetID()] = &a
		result = append(result, &a)
	}
	return result
}

func (m *manager) getProjection(tableID, attrID string) bool {
	for _, v := range m.query.Projections {
		if v.TableId == tableID && v.AttributeId == attrID {
			return true
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
		t := NewTable(v.Pseudoname, v.Table.T, m.getAttributes(*v.Table)...)
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
				Table:      m.memTables[j.TableId],
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

func evaluateQueryPlan(query parser.Query) {
	t, _ := prepareInputQuery(query)
	fmt.Println(query.Name)
	fmt.Printf("%#v", *t[0])
}
