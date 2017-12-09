package parser

import (
	"encoding/json"
	"fmt"
)

type InputParams struct {
	Input int `json:"input"`
	Tables []*Table `json:"Tables"`
	Queries []*Query `json:"Queries"`
}

func (p InputParams) findTable(id string) (*Table, error) {
	for _, v := range p.Tables {
		if v.Id == id {
			return  v, nil
		}
	}
	return nil, fmt.Errorf("can't get table %q in params", id)
}

type Table struct {
	Object
	Name string `json:"Name"` // имя
	T float64 `json:"T"` // количество записей
	L float64 `json:"L"` // количество записей в блоке
	Attributes []*Attribute `json:"Attributes"`
}

func (t Table) findAttr(id string) (*Attribute, error) {
	for _, v := range t.Attributes {
		if v.Id == id {
			return  v, nil
		}
	}
	return nil, fmt.Errorf("can't get attribute %q in table %q", id, t.Id)
}

type Attribute struct {
	Object
	Name string `json:"Name"` // имя
	I float64 `json:"I"` // мощность
}

type Query struct{
	Object
	Name string `json:"Name"` // имя
	Joins []*Join `json:"Joins"`
	Projections []*TableAttribute `json:"Projections"`
	Conditions []*Condition `json:"Conditions"`
}

type Join struct{
	Object
	Join []*TableAttribute `json:"Join"`
}

type TableAttribute struct{
	TableId string `json:"TableId"`
	Table *Table `json:"-"`

	AttributeId string `json:"AttributeId"`
	Attribute *Attribute `json:"-"`
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
	return  o.Id
}

type UniqObject interface {
	GetID() string
}

//func ArrayToDict(arr []UniqObject) (map[string]UniqObject, error) {
//	dict := map[string]UniqObject{}
//
//	for _, v := range arr {
//		key := v.GetID()
//
//		if _, ok := dict[key]; ok {
//			return nil, fmt.Errorf("key %q already exists in dict", key)
//		}
//
//		dict[key] = v
//	}
//
//	return dict, nil
//}
//
//func TableDict(arr []UniqObject) (map[string]Table, error)  {
//	temp, err := ArrayToDict(arr)
//	if err != nil {
//		return nil, err
//	}
//	result := map[string]Table{}
//
//	for k,v := range temp {
//		if value, ok := v.(Table); ok {
//			result[k] = value
//		}
//	}
//	return result, nil
//}

//func (obj InputParams) FindTableById(id string) Table{
//	for _, v := range obj.Tables[:] {
//		if v.Id == id {
//			return v
//		}
//	}
//
//	return Table{}
//}
//
//func (p *InputParams) ChangeInputToZero() {
//	p.Input = 0
//}

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
	for _, q := range p.Queries {
		for _, j := range q.Joins {
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
	return nil
}