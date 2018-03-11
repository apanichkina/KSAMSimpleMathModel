package parser

type GlobalVariables struct {
	D     float64
	D_ind float64
	K     float64
}

type UniqObject interface {
	GetID() string
}

type ID struct {
	ID string `json:"$oid"`
}

type Object struct {
	ID `json:"id"`
}

type InputParams struct {
	ID           `json:"_id"`
	Name         string       `json:"name"`
	DataModel    []*DataModel `json:"datamodel"`
	DataModelMap map[string]*DataModel

	Increment    []*Increment `json:"increment"`
	IncrementMap map[string]*Increment

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

	TransactionID  string `json:"transaction"`
	Transaction    *Transaction
	PossibleValues map[string]IncrementField
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

	FieldName     string  `json:"field"`
	From          float64 `json:"from"`
	Step          float64 `json:"incr"`
	To            float64 `json:"to"`
	PosibleValues []string
	StepsCount    int
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

type Attribute struct {
	Object
	Name string  `json:"name"`   // имя
	I    float64 `json:"attr_I"` // мощность
	Size float64 `json:"size"`   // размер типа атрибута
	PK   bool    `json:"pk"`
}

type TableIDs []*TableInQuery

type TableNames []*TableInQuery

type Query struct {
	Object
	Name string `json:"name"` // имя

	TablesInQuery    []*TableInQuery `json:"tables"` //таблицы с псевдонимами и без, участвующие в запросе
	TablesInQueryMap map[string]*TableInQuery

	Aggregates  []*Aggregate      `json:"aggregates"`
	Joins       []*Join           `json:"joins"`
	Projections []*TableAttribute `json:"projection"`
	Conditions  []*Condition      `json:"condition"`
	Group       []*TableAttribute `json:"group"`
	Order       []*TableAttribute `json:"order"`
}

type Aggregate struct {
	Name string  `json:"name"` // имя
	Size float64 `json:"size"` // размер типа атрибута
}
type TableInQuery struct {
	Object
	Pseudoname string `json:"pseudoname"` // имя

	TableId string `json:"tableid"`
	Table   *Table
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

type Condition struct {
	TableAttribute
	P float64 `json:"P"`
}

type Transaction struct {
	Object
	Name       string              `json:"name"`
	Queries    []*TransactionQuery `json:"queries"`
	QueriesMap map[string]*TransactionQuery
}

type TransactionQuery struct {
	QueryId  string  `json:"queryid"`
	Count    float64 `json:"rep"` // число
	Subquery bool    `json:"sub"` // это подзапрос
}

type IncrementField struct {
	Name   string
	Values []string
}

var INCREMENTVALS = map[string]IncrementField{
	"type":      IncrementField{"Mode", []string{"online", "offline"}},
	"frequency": IncrementField{"Frequency", nil},
	"nodecount": IncrementField{"NodeCount", nil},
	"CPU":       IncrementField{"NodeCount", nil},
	"disk":      IncrementField{"Disk", nil},
	"countdisk": IncrementField{"DiskCount", nil},
}
