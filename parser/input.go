package parser

import (
	"encoding/json"
	"fmt"
	"strings"
)

type InputParams struct {
	Input     int      `json:"input"`
	Tables    []*Table `json:"Tables"`
	TablesMap map[string]*Table

	Queries    []*Query `json:"Queries"`
	QueriesMap map[string]*Query
}

func (p *InputParams) setMaps() {
	p.TablesMap = make(map[string]*Table)
	for _, t := range p.Tables {
		p.TablesMap[t.GetID()] = t
	}

	p.QueriesMap = make(map[string]*Query)
	for _, q := range p.Queries {
		p.QueriesMap[q.GetID()] = q
	}
}

func (p InputParams) findTable(id string) (*Table, error) {
	for _, v := range p.Tables {
		if v.Id == id {
			return v, nil
		}
	}
	return nil, fmt.Errorf("can't get table %q in params", id)
}

type Table struct {
	Object
	Name string  `json:"Name"` // имя
	T    float64 `json:"T"`    // количество записей
	L    float64 `json:"L"`    // количество записей в блоке

	Attributes    []*Attribute `json:"Attributes"`
	AttributesMap map[string]*Attribute
}

type TableIDs []*Table
func (arr TableIDs) String() string {
	var result []string
	for _, v := range arr {
		result = append(result, v.Id)
	}

	return strings.Join(result, ",")
}

func (t *Table) setMaps() {
	t.AttributesMap = make(map[string]*Attribute)
	for _, a := range t.Attributes {
		t.AttributesMap[a.GetID()] = a
	}
}

func (t Table) findAttr(id string) (*Attribute, error) {
	for _, v := range t.Attributes {
		if v.Id == id {
			return v, nil
		}
	}
	return nil, fmt.Errorf("can't get attribute %q in table %q", id, t.Id)
}

func (q Query) FindJoin(leftTableId string, rightTableId string) (bool, string, string) {
	for _, js := range q.Joins {
		var hasLeft = false
		var hasRight = false
		var attrIdLeft = ""
		var attrIdRight = ""
		for _, j := range js.Join {
			if j.TableId == leftTableId {
				hasLeft = true
				attrIdLeft = j.AttributeId
			}
			if j.TableId == rightTableId {
				hasRight = true
				attrIdRight = j.AttributeId
			}
		}
		if hasLeft && hasRight {
			return true, attrIdLeft, attrIdRight
		}
	}
	return false, "", ""
}
//
// правая таблица может быть указана в нескольких джоинах с таблицами из X, поэтому нужно учесть все условия Ex.:p=p1*p2
// не учитывает, что в X могжет содержаться более одной таблицы, содержащей атрибут соединения (а), если учитывать этот момент, то p1=min(I(Qk,a);I(Ql,a)) и анадогично  p2=min(I(Qk,b);I(Ql,b))
func (q Query) GetJoinI(x []*Table, rightTable Table) (float64, float64, error) {
	var I float64 = 1 // I для Y по атрибуту соединения a
	var I_x float64 = 1 // I для X по атрибуту a
	for _, leftTable := range x {
		var hasJoin, attrIdLeft, attrIdRight= q.FindJoin(leftTable.Id, rightTable.Id)
		if hasJoin {
			var joinAttrLeft, okL = leftTable.AttributesMap[attrIdLeft]
			if !okL {
				return 0, 0, fmt.Errorf("can`t find leftattr with id: %s for join tables %s and %s", attrIdLeft, leftTable.Id, rightTable.Id)
			}

			var joinAttrRight, okR = rightTable.AttributesMap[attrIdRight]
			if !okR {
				return 0, 0, fmt.Errorf("can`t find rightattr with id: %s for join tables %s and %s", attrIdRight, leftTable.Id, rightTable.Id)
			}
			I *= joinAttrRight.I
			I_x *= joinAttrLeft.I
		}
	}
	return I, I_x, nil
}


func (q Query) GetAllCondition(table Table) (float64, error) {
	var result float64 = 1
	for _, c := range q.Conditions {
		if c.TableId == table.Id {
			result *= c.P
		}
	}
	return result, nil
}

type Attribute struct {
	Object
	Name string  `json:"Name"` // имя
	I    float64 `json:"I"`    // мощность
}

type Query struct {
	Object
	Name string `json:"Name"` // имя

	Joins    []*Join `json:"Joins"`
	JoinsMap map[string]*Join

	Projections    []*TableAttribute `json:"Projections"`
	ProjectionsMap map[string]*TableAttribute

	Conditions    []*Condition `json:"Conditions"`
	ConditionsMap map[string]*Condition
}

func (q *Query) setMaps() {
	q.JoinsMap = make(map[string]*Join)
	for _, j := range q.Joins {
		q.JoinsMap[j.GetID()] = j
	}

	q.ProjectionsMap = make(map[string]*TableAttribute)
	for _, pr := range q.Projections {
		q.ProjectionsMap[pr.GetID()] = pr
	}

	q.ConditionsMap = make(map[string]*Condition)
	for _, c := range q.Conditions {
		q.ConditionsMap[c.GetID()] = c
	}
}

type Join struct {
	Object

	Join    []*TableAttribute `json:"Join"`
	JoinMap map[string]*TableAttribute
}

func (q *Join) setMaps() {
	q.JoinMap = make(map[string]*TableAttribute)
	for _, j := range q.Join {
		q.JoinMap[j.GetID()] = j
	}
}

type TableAttribute struct {
	TableId string `json:"TableId"`
	Table   *Table `json:"-"`

	AttributeId string     `json:"AttributeId"`
	Attribute   *Attribute `json:"-"`
}

func (c TableAttribute) GetID() string {
	return fmt.Sprintf("%s_%s", c.TableId, c.AttributeId)
}

func (c *TableAttribute) setPointers(ip InputParams) error {
	table, err := ip.findTable(c.TableId)
	if err != nil {
		return fmt.Errorf("can't set pointers [table = %q]: %q", c.TableId, err)
	}
	c.Table = table

	attr, err := table.findAttr(c.AttributeId)
	if err != nil {
		return fmt.Errorf("can't set pointers [attribute = %q]: %q", c.AttributeId, err)
	}
	c.Attribute = attr

	return nil
}

type Condition struct {
	TableAttribute
	P float64 `json:"P"`
}

type Object struct {
	Id string `json:"Id"`
}

func (o Object) GetID() string {
	return o.Id
}

type UniqObject interface {
	GetID() string
}

func GetInputParamsFromString(input string) (InputParams, error) {
	data := []byte(input)

	var result InputParams
	err := json.Unmarshal(data, &result)
	if err != nil {
		return InputParams{}, err
	}

	err = result.PrepareData()
	if err != nil {
		return InputParams{}, err
	}
	return result, nil
}

func (p *InputParams) PrepareData() error {
	p.setMaps()

	for _, q := range p.Queries {
		q.setMaps()

		for _, j := range q.Joins {
			j.setMaps()

			for _, join := range j.Join {
				err := join.setPointers(*p)
				if err != nil {
					return err
				}
			}
		}

		for _, cond := range q.Conditions {
			err := cond.setPointers(*p)
			if err != nil {
				return err
			}
		}

		for _, proj := range q.Projections {
			err := proj.setPointers(*p)
			if err != nil {
				return err
			}
		}
	}

	for _, t := range p.Tables {
		t.setMaps()
	}

	return nil
}
