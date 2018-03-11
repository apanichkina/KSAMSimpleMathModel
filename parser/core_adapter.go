package parser

import (
	"fmt"
	"math"
)

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

	var canSearchJoinSequence = len(result) == 0 && hasLeftInAll && hasRightInAll

	return result, canSearchJoinSequence, nil
}

func (q Query) GetRowSizeAfterProjection(table *TableInQuery) float64 {
	var result float64 = 0

	for _, p := range q.Projections {
		if p.TableId == table.GetID() {
			var attr, hasAttr = table.Table.AttributesMap[p.AttributeId]
			if hasAttr {
				result += attr.Size
			}

		}
	}
	// TODO ошибка

	return result
}

func (q Query) GetRowCountAfterGroupBy() float64 {
	var result float64 = 1
	var hasGroupBy = false

	for _, g := range q.Group {
		var table, hasTable = q.TablesInQueryMap[g.TableId]
		if hasTable { // модель может содержать ссылки на таблицы не из этого запроса, это нужно валидировать на клиенте до модели, но ту проверять дешевле
			var attr, hasAttr = table.Table.AttributesMap[g.AttributeId]
			if hasAttr {
				result *= attr.I
				hasGroupBy = true
			}
		}
	}
	if hasGroupBy {
		return result
	}

	return math.MaxFloat64
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
