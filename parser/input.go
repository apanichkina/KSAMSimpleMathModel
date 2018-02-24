package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"math"
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

type InputParams struct {
	ID        `json:"_id"`
	Name      string       `json:"name"`
	DataModel []*DataModel `json:"datamodel"`

	Increment []*Increment `json:"increment"`

	Nodes	[]*Node `json:"nodes"`
	//NodesMap map[string]*Node
	Networks	[]*Network `json:"networks"`
}

type Increment struct {
	Object
	ObjId	string   `json:"obj"`
	ObjType	string	`json:"objtype"`

	FieldName	string   `json:"field"`
	From	float64   `json:"from"`
	Step	float64   `json:"incr"`
	To	float64   `json:"to"`
}

type Node struct {
	Object
	Name      string   `json:"name"`
	Type      string   `json:"node_type"`
	NodeCount      float64   `json:"nodecount"`
	Mode      string   `json:"mode"`
	Mem      float64   `json:"mem"`
	Disk      float64   `json:"disk"`
	DiskCount     float64   `json:"diskcount"`
	Net      float64   `json:"net"`
	Proc      float64   `json:"proc"`
}

type Network struct {
	Object
	Name      string   `json:"name"`
	NodesID	[]string `json:"nodes"`
	Speed      float64   `json:"speed"`
}

type DataModel struct {
	Object
	Name      string   `json:"name"`
	Tables    []*Table `json:"tables"`
	TablesMap map[string]*Table

	Queries    []*Query `json:"queries"`
	QueriesMap map[string]*Query

	Transactions    []*Transaction `json:"transactions"`
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
	Name string            `json:"name"`  // имя
	T    float64           `json:"nrows"` // количество записей
	L    float64 `json:"L"`     // количество записей в блоке

	Attributes    []*Attribute `json:"attributes"`
	AttributesMap map[string]*Attribute

	PKAttribute *Attribute

	Size	float64		// Длина записи в байтах

}

func (t *Table) UnmarshalJSON(data []byte) error {
	type alias Table
	var temp alias

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("can't unmarshal to %T: %s", t, err)
	}

	if temp.L == 0 {
		temp.L = 200
	}
	*t = Table(temp)
	return nil
}

type Attribute struct {
	Object
	Name string  `json:"name"` // имя
	I    float64 `json:"attr_I"`    // мощность
	Size float64 `json:"size"` // размер типа атрибута
	PK bool `json:"pk"`
}

func (a *Attribute) UnmarshalJSON(data []byte) error {
	type alias Attribute
	var temp alias

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return fmt.Errorf("can't unmarshal to %T: %s", a, err)
	}

	if temp.I == 0 {
		temp.I = 200
	}
	*a = Attribute(temp)
	return nil
}

type TableIDs []*TableInQuery

func (arr TableIDs) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.GetID())
	}

	return strings.Join(result, ",")
}

type TableNames []*TableInQuery

func (arr TableNames) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.Table.Name)
	}

	return strings.Join(result, ",")
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

type Query struct {
	Object
	Name string `json:"name"` // имя

	TablesInQuery    []*TableInQuery `json:"tables"` //таблицы с псевдонимами и без, участвующие в запросе
	TablesInQueryMap map[string]*TableInQuery

	Joins       []*Join           `json:"joins"`
	Projections []*TableAttribute `json:"projection"`
	Conditions  []*Condition      `json:"condition"`
}

type TableInQuery struct {
	Object
	Pseudoname string `json:"pseudoname"` // имя

	TableId string `json:"tableid"`
	Table   *Table
}

func (q *Query) setMaps() {
	q.TablesInQueryMap = make(map[string]*TableInQuery)
	for _, t := range q.TablesInQuery {
		q.TablesInQueryMap[t.GetID()] = t
	}
}

type Join struct {
	Object
	Join []*TableAttributes `json:"join"`
}

type JoinAttributes struct {
	LeftAttrId []string
	RightAttrId []string
}

type TableAttributes struct {
	TableId    string   `json:"tableid"`
	Attributes []string `json:"attributes"`
}

type TableAttribute struct {
	TableId     string `json:"tableid"`
	AttributeId string `json:"attributeId"`
}

func (c TableAttribute) GetID() string {
	return fmt.Sprintf("%s_%s", c.TableId, c.AttributeId)
}

type Condition struct {
	TableAttribute
	P float64 `json:"P"`
}

func (q Query) FindJoins(leftTableId string, rightTableId string) ([]JoinAttributes, error) {
	var result []JoinAttributes
	for _, js := range q.Joins {
		var hasLeft = false
		var hasRight = false
		var attrsIdLeft []string
		var attrsIdRight []string
		for _, j := range js.Join {
			if len(j.Attributes) < 1 {
				return  nil, fmt.Errorf("too few join attrs in table (%s) in query (%s)", j.TableId, q.Name)
			}
			if j.TableId == leftTableId {
				hasLeft = true
				attrsIdLeft = j.Attributes
			}
			if j.TableId == rightTableId {
				hasRight = true
				attrsIdRight = j.Attributes
			}
		}
		if hasLeft && hasRight {
			result = append(result, JoinAttributes{attrsIdLeft, attrsIdRight})
		}
	}

	return result, nil
}

//
// правая таблица может быть указана в нескольких джоинах с таблицами из X, поэтому нужно учесть все условия Ex.:p=p1*p2
// не учитывает, что в X могжет содержаться более одной таблицы, содержащей атрибут соединения (а), если учитывать этот момент, то p1=min(I(Qk,a);I(Ql,a)) и анадогично  p2=min(I(Qk,b);I(Ql,b))
func (q Query) GetJoinAttr(x []*TableInQuery, rightTable TableInQuery, N float64) (*Attribute, float64, float64, error) {
	var I float64 = 0   // I для Y по атрибуту соединения a
	var P_maxI float64 = 1 // Вероятность P для текущего I
	var P float64 = 1	// P для Y по условиям join, по которым не читается таблица
	var Attr *Attribute = nil
	var JoinLeftI float64 = 0
	for _, leftTable := range x {
		var joinAttrs, err = q.FindJoins(leftTable.GetID(), rightTable.GetID())
		if err != nil {
			return Attr, P, JoinLeftI, err
		}
		// проход  по джоинам, ищем соединение, где максимальный I
		for _, ja := range joinAttrs {
			var idL = ja.LeftAttrId[0]
			var joinAttrLeft, okL = leftTable.Table.AttributesMap[idL]
			if !okL {
				return Attr, P, JoinLeftI, fmt.Errorf("can`t find leftattr with id: %s for join tables %s and %s", idL, leftTable.Table.GetID(), rightTable.Table.GetID())
			}

			var idR = ja.RightAttrId[0]
			var joinAttrRight, okR = rightTable.Table.AttributesMap[idR]
			if !okR {
				return Attr, P, JoinLeftI, fmt.Errorf("can`t find rightattr with id: %s for join tables %s and %s", idR, leftTable.Table.GetID(), rightTable.Table.GetID())
			}

			var leftI = math.Min(joinAttrLeft.I, N)
			var currentP = math.Min(leftI, joinAttrRight.I) / math.Max(leftI, joinAttrRight.I)

			// ищем атрибут с максимальним I, чтобы 1/I было маленьким
			// Остальные атрибуты будут в P
			if joinAttrRight.I > I {
				I = joinAttrRight.I
				Attr = joinAttrRight
				P_maxI = currentP
				JoinLeftI = leftI
			}


			P *= currentP
		}

	}
	P /= P_maxI
	return Attr, P, JoinLeftI, nil
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
	Name       string              `json:"name"`
	Queries    []*TransactionQuery `json:"queries"`
	QueriesMap map[string]*TransactionQuery
}

type TransactionQuery struct {
	QueryId string `json:"queryid"`
	Count   string `json:"rep"` // число
}

func (o TransactionQuery) GetID() string {
	return o.QueryId
}

func (q *Transaction) setMaps() error{
	if len(q.Queries) == 0 {
		return fmt.Errorf("transacton %s has no queries", q.Name)
	}
	q.QueriesMap = make(map[string]*TransactionQuery)
	for _, t := range q.Queries {
		q.QueriesMap[t.GetID()] = t
	}
	return nil
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
			err := t.setMaps()
			if err != nil {
				return err
			}
		}

		for _, t := range p.Transactions {
			err := t.setMaps()
			if err != nil {
				return err
			}
		}

	}

	return nil
}
