package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"strings"
)

type GlobalVariables struct {
	D     float64
	D_ind float64
	K     float64
}

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
	ID           `json:"_id"`
	Name         string       `json:"name"`
	DataModel    []*DataModel `json:"datamodel"`
	DataModelMap map[string]*DataModel

	Increment []*Increment `json:"increment"`

	Node    []*Node `json:"nodes"`
	NodeMap map[string]*Node

	Network     []*Network  `json:"networks"`
	Database    []*Database `json:"database"`
	DatabaseMap map[string]*Database
	Request     []*Request `json:"requests"`
}

type Request struct {
	Object
	Name      string  `json:"name"`
	Mode      string  `json:"mode"`
	Frequency float64 `json:"frequency"`

	NodeID string `json:"node"` // узел обращения
	Node   *Node

	DatabaseID string `json:"database"`
	Database   *Database

	TransactionID string `json:"transaction"`
	Transaction   *Transaction
}

type Database struct {
	Object
	Name string `json:"name"`

	NodeID string `json:"node"` // кластер размещения
	Node   *Node

	DatamodelID string `json:"datamodel"`
	DataModel   *DataModel
}

type Increment struct {
	Object
	ObjId   string `json:"obj"`
	ObjType string `json:"objtype"`

	FieldName string  `json:"field"`
	From      float64 `json:"from"`
	Step      float64 `json:"incr"`
	To        float64 `json:"to"`
}

type Node struct {
	Object
	Name      string  `json:"name"`
	Type      string  `json:"node_type"`
	NodeCount float64 `json:"nodecount"`
	Mode      string  `json:"mode"`
	Mem       float64 `json:"mem"`
	Disk      float64 `json:"disk"`
	DiskCount float64 `json:"diskcount"`
	Net       float64 `json:"net"`
	Proc      float64 `json:"proc"`
}

type Network struct {
	Object
	Name    string   `json:"name"`
	NodesID []string `json:"nodes"`
	Speed   float64  `json:"speed"`
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
		x, okX := ip.NodeMap[d.NodeID]
		if !okX {
			return fmt.Errorf("can't find node by id %s in request: %s", d.NodeID, d.GetID())
		}
		d.Node = x

		y, okY := ip.DatabaseMap[d.DatabaseID]
		if !okY {
			return fmt.Errorf("can't find database by id %s in request: %s", d.DatabaseID, d.GetID())
		}
		d.Database = y

		z, okZ := d.Database.DataModel.TransactionsMap[d.TransactionID]
		if !okZ {
			return fmt.Errorf("can't find transaction by id %s in request: %s", d.TransactionID, d.GetID())
		}
		d.Transaction = z
	}

	return nil
}
func (d *InputParams) setMaps() {
	d.DataModelMap = make(map[string]*DataModel)
	for _, t := range d.DataModel {
		d.DataModelMap[t.GetID()] = t
	}

	d.NodeMap = make(map[string]*Node)
	for _, t := range d.Node {
		d.NodeMap[t.GetID()] = t
	}

	d.DatabaseMap = make(map[string]*Database)
	for _, t := range d.Database {
		d.DatabaseMap[t.GetID()] = t
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

type Table struct {
	Object
	Name string  `json:"name"`  // имя
	T    float64 `json:"nrows"` // количество записей
	L    float64 `json:"L"`     // количество записей в блоке

	Attributes    []*Attribute `json:"attributes"`
	AttributesMap map[string]*Attribute

	PKAttribute *Attribute

	Size float64 // Длина записи в байтах

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
	Name string  `json:"name"`   // имя
	I    float64 `json:"attr_I"` // мощность
	Size float64 `json:"size"`   // размер типа атрибута
	PK   bool    `json:"pk"`
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
	LeftAttrId  []string
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

func (q Query) FindJoins(leftTableId string, rightTableId string) ([]JoinAttributes, bool, error) {
	var result []JoinAttributes
	var hasLeftInAll = false
	var hasRightInAll = false
	var JoinsForSequence []*Join
	for _, js := range q.Joins {
		var hasLeft = false
		var hasRight = false
		var attrsIdLeft []string
		var attrsIdRight []string
		for _, j := range js.Join {
			if len(j.Attributes) < 1 {
				return nil, false, fmt.Errorf("too few join attrs in table (%s) in query (%s)", j.TableId, q.Name)
			}
			if j.TableId == leftTableId {
				hasLeft = true
				hasLeftInAll = true
				attrsIdLeft = j.Attributes
			}
			if j.TableId == rightTableId {
				hasRight = true
				hasRightInAll = true
				attrsIdRight = j.Attributes
			}
		}
		if hasLeft && hasRight {
			result = append(result, JoinAttributes{attrsIdLeft, attrsIdRight})
		} else if hasLeft || hasRight {
			JoinsForSequence = append(JoinsForSequence, js)
		}
	}
	//if hasLeftInAll && hasRightInAll && len(JoinsForSequence) > 1 { // проверяем, что в JoinsForSequence есть joins с левой и правой таблицей
	//	var tempTebleID string = ""
	//	for _, jfs := range JoinsForSequence {
	//		for _, j := range jfs.Join {
	//			if len(j.Attributes) < 1 {
	//				return nil, false, fmt.Errorf("too few join attrs in table (%s) in query (%s)", j.TableId, q.Name)
	//			}
	//			if j.TableId != leftTableId && j.TableId != rightTableId {
	//				if tempTebleID == "" {
	//					tempTebleID = j.TableId
	//				} else if j.TableId != tempTebleID
	//
	//			}
	//		}
	//
	//	}
	//}

	// fmt.Println(JoinsForSequence)
	var canSearchJoinSequence = len(result) == 0 && hasLeftInAll && hasRightInAll

	return result, canSearchJoinSequence, nil
}

func (q Query) GetRowSizeAfterProjection(table *TableInQuery, attrJoin *Attribute) float64 {
	var result float64 = 0
	var hasJoin = false

	for _, p := range q.Projections {
		if p.TableId == table.GetID() {
			var attrID = p.AttributeId
			if attrJoin != nil && attrID == attrJoin.GetID() {
				hasJoin = true
			}
			var size = table.Table.AttributesMap[attrID].Size
			result += size
		}
	}
	if hasJoin {
		result -= attrJoin.Size
	}
	// TODO ошибка

	return result
}

//
// правая таблица может быть указана в нескольких джоинах с таблицами из X, поэтому нужно учесть все условия Ex.:p=p1*p2
// не учитывает, что в X может содержаться более одной таблицы, содержащей атрибут соединения (а), если учитывать этот момент, то p1=min(I(Qk,a);I(Ql,a)) и анадогично  p2=min(I(Qk,b);I(Ql,b))
func (q Query) GetJoinAttr(x []*TableInQuery, rightTable TableInQuery, N float64) (*Attribute, float64, float64, error) {
	var I float64 = 0      // I для Y по атрибуту соединения a
	var P_maxI float64 = 1 // Вероятность P для текущего I
	var P float64 = 1      // P для Y по условиям join, по которым не читается таблица
	var Attr *Attribute = nil
	var JoinLeftI float64 = 0
	for _, leftTable := range x {
		var joinAttrs, canSearchJoinSequence, err = q.FindJoins(leftTable.GetID(), rightTable.GetID())
		if err != nil {
			return Attr, P, JoinLeftI, err
		}
		if canSearchJoinSequence {
			// fmt.Println("Нет явного соединения таблиц %s, %s. Но есть неяное. ", leftTable.Pseudoname, rightTable.Pseudoname)
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
			var currentP = math.Min(leftI, joinAttrRight.I) / math.Max(leftI, joinAttrRight.I) // TODO спорно

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
	QueryId string  `json:"queryid"`
	Count   float64 `json:"rep"` // число
}

func (o TransactionQuery) GetID() string {
	return o.QueryId
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

var findNumsInQuotes = regexp.MustCompile(`"([-+]?)\d*\.\d+"|"\d+"`)
var findEmptyStrings = regexp.MustCompile(`(\s)*,*(\s)*("[^"]*")([:])(\s)*("")(\s)*,*(\s)*`)
var findEndOrStartCommas = regexp.MustCompile(`{(\s)*,|,(\s)*}`)

func prepareNumsInQuotes(input []byte) []byte {
	return []byte(findNumsInQuotes.ReplaceAllStringFunc(string(input), func(input string) string {
		if input == `""` {
			return "0"
		}
		return strings.Replace(input, `"`, ``, -1)
	}))
}

func prepareEmptyQuotes(input []byte) []byte {
	return []byte(findEmptyStrings.ReplaceAllStringFunc(string(input), func(input string) string {
		workingStr := strings.TrimSpace(input)
		hasPreComma := strings.HasPrefix(workingStr, ",")
		hasPostComma := strings.HasSuffix(workingStr, ",")
		switch {
		case hasPreComma && hasPostComma: // ..., "a": "",... -> ..., ...
			return ", "
		default:
			return ""
		}
	}))
}

func prepareEndStartCommas(input []byte) []byte {
	return []byte(findEndOrStartCommas.ReplaceAllStringFunc(string(input), func(input string) string {
		if strings.Contains(input, "{") {
			return "{"
		}
		return "}"
	}))
}

func PrepareJson(input []byte) []byte {
	return prepareEndStartCommas(
		prepareEmptyQuotes(
			prepareNumsInQuotes(input),
		),
	)
}

func GetInputParamsFromByteSlice(input []byte) (InputParams, error) {
	var result InputParams

	input = PrepareJson(input)

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
	ip.setMaps()
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

	err := ip.setPointers()
	if err != nil {
		return err
	}

	return nil
}
