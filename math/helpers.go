package math

import (
	"fmt"
	"math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

func TableScan(Table parser.Table, C_filter float64, C_b float64) (float64, float64, error) {
	if Table.Size == 0 {
		return 0.0, 0.0, fmt.Errorf("%s Table.Size cann`t be 0", Table.Name)
	}
	var T float64 = Table.T
	var B float64 = GLOBALVARS.D / Table.Size
	var C_cpu float64 = T * C_filter
	var C_io float64 = B * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}

func IndexScanRead(Table parser.Table, Query parser.Query, C_filter float64, C_b float64) (float64, float64, error) { //Допущение: Индекс не кластеризован! И индекс только на PK
	if Table.PKAttribute == nil {
		return math.MaxFloat64, 0.0, nil
	}
	var tableId = Table.GetID()
	var pkId = Table.PKAttribute.GetID()
	var p float64 = 1
	for _, c := range Query.Conditions {
		if c.TableId == tableId && c.AttributeId == pkId {
			p *= c.P
		}
	}
	if p == 1 {
		return math.MaxFloat64, 0.0, nil
	}
	var T float64 = Table.T * p
	var B_ind float64 = GLOBALVARS.D_ind / Table.PKAttribute.Size
	var C_cpu float64 = T * C_filter
	var C_io float64 = (math.Ceil(B_ind*p) + math.Ceil(Table.T*p)) * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}

func JoinPlanRead(Table parser.Table, Attr *parser.Attribute, N float64, C_filter float64, C_b float64) (float64, float64, float64, error) { //Допущение: Индекс не кластеризован!
	var I = Attr.I
	if I == 0 {
		return 0.0, 0.0, 0.0, fmt.Errorf("%s Attr.I cann`t be 0 for join. ", Table.Name)
	}

	var T float64 = Table.T * 1 / I
	var C_cpu float64 = N * T * C_filter
	var B_ind float64 = math.Ceil(T * (Attr.Size / GLOBALVARS.D_ind))
	var C_io float64 = N * (B_ind + math.Ceil(Table.T*1/I)) * C_b
	var C = C_cpu + C_io

	return C, C_io, T, nil
}

type QueryTimesCache map[string]parser.QueriesMinTimes // key = datamodel.id + '_' node.id

func getQueryTimesCacheID(datamodel *parser.DataModel, nodeID string) string {
	return fmt.Sprint(datamodel.GetID(), "_", nodeID)
}

func getResultRowCountAndSize(query *parser.Query, readRowCount float64) (float64, float64, float64, error){ //grop by, aggregate, order by
	if readRowCount == 0 {
		return 0.0, 0.0, 0.0, fmt.Errorf("Query %s result is empty after read and filter. ", query.Name)
	}
	var orderTime float64 = 0
	var resultRowCount float64 = 0
	var resultRowSize float64 = 0
	var groupsCount float64 = float64(len(query.GroupMap))
	var orderCount float64 = float64(len(query.OrderMap))

	// group by
	if groupsCount > 0 {
		orderTime += groupsCount * C_filter * C_order * readRowCount * math.Log2(readRowCount)
		resultRowCount = math.Min(query.GetRowCountAfterGroupBy(), readRowCount)
	}

	// agregate
	for _, aggregate := range query.Aggregates {
		resultRowSize += aggregate.Size
	}
	if groupsCount == 0 {
		if resultRowSize != 0 {
			// нет группировки, но есть агрегация
			resultRowCount = 1
		} else {
			// нет группировки и агрегации
			resultRowCount = readRowCount
		}
	}

	// order
	if orderCount > 0 {
		orderTime += orderCount * C_filter * C_order * readRowCount * math.Log2(readRowCount)
	}

	// projection
	for _, p := range query.ProjectionsMap {
		var attr = query.TablesInQueryMap[p.TableId].Table.AttributesMap[p.AttributeId]
		resultRowSize += attr.Size
	}

	return resultRowCount, resultRowSize, orderTime, nil
}
