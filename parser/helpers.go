package parser

import (
	"fmt"
)

//////// NOT USED ///////

func (a *Increment) getIncrementID() string {
	return fmt.Sprint(a.ObjId, "_", a.FieldName)
}

func (c TableAttribute) GetID() string {
	return fmt.Sprintf("%s_%s", c.TableId, c.AttributeId)
}



//////// NOT USED ///////

func (o ID) GetID() string {
	return o.ID
}

func (o TransactionQuery) GetID() string {
	return o.QueryId
}

func (ip *InputParams) setPointers() error {
	for _, d := range ip.Database {
		x, okX := ip.NodeMap[d.NodeID]
		if !okX {
			return fmt.Errorf("can't find node by id %s in database: %s", d.NodeID, d.GetID())
		}
		d.Node = x

		y, okY := ip.DataModelMap[d.DatamodelID]
		if !okY {
			return fmt.Errorf("can't find datamodel by id %s in database: %s", d.DatamodelID, d.GetID())
		}
		d.DataModel = y
	}

	for _, d := range ip.Request {
		y, okY := ip.DatabaseMap[d.DatabaseID]
		if !okY {
			return fmt.Errorf("can't find database by id %s in request: %s", d.DatabaseID, d.GetID())
		}
		d.Database = y

		var nodeID = d.NodeID
		if nodeID == "" {
			d.Node = y.Node
		} else {
			x, okX := ip.NodeMap[nodeID]
			if !okX {
				return fmt.Errorf("can't find node by id %s in request: %s", d.NodeID, d.GetID())
			}
			d.Node = x
		}

		z, okZ := d.Database.DataModel.TransactionsMap[d.TransactionID]
		if !okZ {
			return fmt.Errorf("can't find transaction by id %s in request: %s", d.TransactionID, d.GetID())
		}
		d.Transaction = z
	}

	return nil
}

func (a *TableInQuery) setPointers(ip DataModel) error {
	table, err := ip.findTable(a.TableId)
	if err != nil {
		return fmt.Errorf("can't set pointers [table = %q]: %q", a.TableId, err)
	}
	a.Table = table

	return nil
}

func (d *InputParams) setMaps() {
	d.DataModelMap = make(map[string]*DataModel)
	for _, t := range d.DataModel {
		d.DataModelMap[t.GetID()] = t
	}

	d.IncrementMap = make(map[string]*Increment)
	for _, t := range d.Increment {
		d.IncrementMap[t.ObjId] = t
	}

	d.NodeMap = make(map[string]*Node)
	for _, t := range d.Node {
		d.NodeMap[t.GetID()] = t
	}

	d.DatabaseMap = make(map[string]*Database)
	for _, t := range d.Database {
		d.DatabaseMap[t.GetID()] = t
	}

	for _, i := range d.Increment {
		var incrementvals, ok = INCREMENTVALS[i.FieldName]
		if !ok {
			panic("Can't find such increment field")
		}
		i.FieldName = incrementvals.Name
		i.PosibleValues = incrementvals.Values
		if incrementvals.Values != nil {
			i.StepsCount = len(incrementvals.Values)
		} else {
			i.StepsCount = int((i.To - i.From) / i.Step)
		}
	}

}

func (d *DataModel) setMaps() {
	d.TablesMap = make(map[string]*Table)
	for _, t := range d.Tables {
		d.TablesMap[t.GetID()] = t
	}

	d.QueriesMap = make(map[string]*Query)
	for _, q := range d.Queries {
		d.QueriesMap[q.GetID()] = q
	}

	d.TransactionsMap = make(map[string]*Transaction)
	for _, t := range d.Transactions {
		d.TransactionsMap[t.GetID()] = t
	}
}

func (t *Table) setMaps() error {
	if len(t.Attributes) == 0 {
		return fmt.Errorf("table %s has no attributes", t.Name)
	}
	t.AttributesMap = make(map[string]*Attribute)

	var size float64 = 0

	for _, a := range t.Attributes {
		t.AttributesMap[a.GetID()] = a
		size += a.Size
		if a.PK {
			t.PKAttribute = a
		}
	}
	t.Size = size

	return nil
}

func (q *Query) setMaps() {
	q.TablesInQueryMap = make(map[string]*TableInQuery)
	for _, t := range q.TablesInQuery {
		q.TablesInQueryMap[t.GetID()] = t
	}
}

func (q *Transaction) setMaps() error {
	if len(q.Queries) == 0 {
		return fmt.Errorf("transacton %s has no queries", q.Name)
	}
	q.QueriesMap = make(map[string]*TransactionQuery)
	for _, t := range q.Queries {
		q.QueriesMap[t.GetID()] = t
	}
	return nil
}




