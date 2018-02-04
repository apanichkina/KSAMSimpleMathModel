package parser

import (
	"encoding/json"
	"fmt"
	"strings"
	"io/ioutil"
)

type ID struct {
	ID string `json:"$oid"`
}

func (o ID) GetID() string {
	return o.ID
}

type Object struct {
	ID `json:"id"`
}

type UniqObject interface {
	GetID() string
}

type InputParams struct{
	ID `json:"_id"`
	DataModel []*DataModel	`json:"datamodel"`
}


type DataModel struct {
	Object
	Tables    []*Table `json:"tables"`
	TablesMap map[string]*Table

	Queries    []*Query `json:"queries"`
	QueriesMap map[string]*Query

	Transactions []*Transaction `json:"transactions"`
	TransactionsMap map[string]*Transaction
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

type Table struct {
	Object
	Name string  `json:"name"`		// имя
	T    float64 `json:"nrows"`  	// количество записей
	L    float64 `json:"L"`    		// количество записей в блоке

	Attributes    []*Attribute `json:"attributes"`
	AttributesMap map[string]*Attribute
}

type Attribute struct {
	Object
	Name string  `json:"name"` // имя
	I    float64 `json:"I"`    // мощность
}


type TableIDs []*TableInQuery

func (arr TableIDs) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.GetID())
	}

	return strings.Join(result, ",")
}

func (t *Table) setMaps() {
	t.AttributesMap = make(map[string]*Attribute)
	for _, a := range t.Attributes {
		t.AttributesMap[a.GetID()] = a
	}
}

type Query struct {
	Object
	Name string `json:"name"` // имя

	TablesInQuery []*TableInQuery `json:"tables"` //таблицы с псевдонимами и без, участвующие в запросе
	TablesInQueryMap map[string]*TableInQuery

	Joins    []*Join					`json:"joins"`
	Projections    []*TableAttribute	`json:"projection"`
	Conditions    []*Condition			`json:"condition"`
}

type TableInQuery struct {
	Object
	Pseudoname string  `json:"pseudoname"` // имя

	TableId string `json:"tableid"`
	Table *Table
}

func (q *Query) setMaps() {
	q.TablesInQueryMap = make(map[string]*TableInQuery)
	for _, t := range q.TablesInQuery {
		q.TablesInQueryMap[t.GetID()] = t
	}
}

type Join struct {
	Object
	Join    []*TableAttributes `json:"join"`
}

type TableAttributes struct {
	TableId string		`json:"tableid"`
	Attributes []string	`json:"attributes"`
}

type TableAttribute struct {
	TableId string		`json:"tableid"`
	AttributeId string	`json:"attributeId"`
}

func (c TableAttribute) GetID() string {
	return fmt.Sprintf("%s_%s", c.TableId, c.AttributeId)
}

type Condition struct {
	TableAttribute
	P float64 `json:"P"`
}

func (q Query) FindJoin(leftTableId string, rightTableId string) (bool, []string, []string, error) {
	for _, js := range q.Joins {
		var hasLeft = false
		var hasRight = false
		var attrsIdLeft []string
		var attrsIdRight []string
		for _, j := range js.Join {
			if j.TableId == leftTableId {
				hasLeft = true
				attrsIdLeft = j.Attributes
				if len(attrsIdLeft) < 1 {
					return false, nil, nil, fmt.Errorf("too few join attrs in table (%s) in query (%s)", j.TableId, q.Name)
				}
			}
			if j.TableId == rightTableId {
				hasRight = true
				attrsIdRight = j.Attributes
				if len(attrsIdRight) < 1 {
					return false, nil, nil, fmt.Errorf("too few join attrs in table (%s) in query (%s)", j.TableId, q.Name)
				}
			}
		}
		if hasLeft && hasRight {
			return true, attrsIdLeft, attrsIdRight, nil
		}
	}
	return false, nil, nil, nil
}
//
// правая таблица может быть указана в нескольких джоинах с таблицами из X, поэтому нужно учесть все условия Ex.:p=p1*p2
// не учитывает, что в X могжет содержаться более одной таблицы, содержащей атрибут соединения (а), если учитывать этот момент, то p1=min(I(Qk,a);I(Ql,a)) и анадогично  p2=min(I(Qk,b);I(Ql,b))
func (q Query) GetJoinI(x []*TableInQuery, rightTable TableInQuery) (float64, float64, error) {
	var I float64 = 1 // I для Y по атрибуту соединения a
	var I_x float64 = 1 // I для X по атрибуту a
	for _, leftTable := range x {
		var hasJoin, attrIdLeft, attrIdRight, err = q.FindJoin(leftTable.GetID(), rightTable.GetID())
		if err != nil {
			return 0, 0, err
		}
		if hasJoin {
			for _, id := range attrIdLeft {
				var joinAttrLeft, okL = leftTable.Table.AttributesMap[id]
				if !okL {
					return 0, 0, fmt.Errorf("can`t find leftattr with id: %s for join tables %s and %s", id, leftTable.Table.GetID(), rightTable.Table.GetID())
				}
				I_x *= joinAttrLeft.I
			}

			for _, id := range attrIdRight {
				var joinAttrRight, okR = rightTable.Table.AttributesMap[id]
				if !okR {
					return 0, 0, fmt.Errorf("can`t find rightattr with id: %s for join tables %s and %s", id, leftTable.Table.GetID(), rightTable.Table.GetID())
				}
				I *= joinAttrRight.I
			}
		}
	}
	return I, I_x, nil
}


func (q Query) GetAllCondition(tableId string) (float64, error) {
	var result float64 = 1
	for _, c := range q.Conditions {
		if c.TableId == tableId {
			result *= c.P
		}
	}
	return result, nil
}

func (p DataModel) findTable(id string) (*Table, error) {
	var table, ok = p.TablesMap[id]
	if ok {
		return table, nil
	}

	return nil, fmt.Errorf("can't get table %q in params", id)
}

func (c *TableInQuery) setPointers(ip DataModel) error {
	table, err := ip.findTable(c.TableId)
	if err != nil {
		return fmt.Errorf("can't set pointers [table = %q]: %q", c.TableId, err)
	}
	c.Table = table

	return nil
}

type Transaction struct {
	 Object
	 Name string `json:"name"`
	 Queries []*TransactionQuery `json:"queries"`
	 QueriesMap map[string]*TransactionQuery
}

type TransactionQuery struct {
	QueryId string `json:"queryid"`
	Count int `json:"rep"`          // число
}

func (o TransactionQuery) GetID() string {
	return o.QueryId
}

func (q *Transaction) setMaps() {
	q.QueriesMap = make(map[string]*TransactionQuery)
	for _, t := range q.Queries {
		q.QueriesMap[t.GetID()] = t
	}
}

func GetInputParamsFromByteSlice(input []byte) (InputParams, error) {
	var result InputParams
	err := json.Unmarshal(input, &result)
	if err != nil {
		return InputParams{}, fmt.Errorf("can't unmarshal: %s", err)
	}

	err = result.PrepareData()
	if err != nil {
		return InputParams{}, fmt.Errorf("can't prepare data: %s", err)
	}
	return result, nil
}

func GetInputParamsFromString(input string) (InputParams, error) {
	data := []byte(input)
	return GetInputParamsFromByteSlice(data)
}

func GetInputParamsFromFile(inputFile string) (InputParams, error) {
	raw, err := ioutil.ReadFile(inputFile)
	if err != nil {
		return InputParams{}, fmt.Errorf("can't get file from %q: %s", inputFile, err)
	}
	return GetInputParamsFromByteSlice(raw)
}

func (ip *InputParams) PrepareData() error {
	for _, p := range ip.DataModel {
		p.setMaps()

		for _, q := range p.Queries {
			q.setMaps()

			for _, tq := range q.TablesInQuery {
				err := tq.setPointers(*p)
				if err != nil {
					return err
				}
			}
		}

		for _, t := range p.Tables {
			t.setMaps()
		}

		for _, t := range p.Transactions {
			t.setMaps()
		}

	}

	return nil
}
