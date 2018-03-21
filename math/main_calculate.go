package math

import (
	"fmt"
	"math"
	"reflect"

	"github.com/apanichkina/KSAMSimpleMathModel/helper"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

func EvaluateQueries(params *parser.DataModel, C_filter float64, C_b float64, n_proc float64) (parser.QueriesMinTimes, error) {

	// fmt.Println("C_filter", C_filter, "C_b", C_b)
	// fmt.Println("Считается концептуальная модель: ", params.Name)
	// подготовительные расчеты для входных параметров

	if len(params.Queries) < 1 {
		return nil, fmt.Errorf("can`t find any query")
	}
	var queriesMinTime parser.QueriesMinTimes // минимальное время выполнения всех запросов
	// проход по всем запоросам
	for _, query := range params.Queries {
		if TEST && query.Name != TESTQUERYNAME {
			continue
		}
		var queryMinTime float64 = -1  // минимальное время выполнения запроса
		var queryMinTimeIO float64 = 0 // минимальное время выполнения запроса
		var readRowCount float64 = 0

		if len(query.Joins) == 0 {
			// нет джоинов -> это простой запрос
			if len(query.TablesInQueryMap) == 0 {
				return nil, fmt.Errorf("too few tabels in query %s", query.Name)
			}
			var T_x float64 = 1
			var Z_x float64 = 0
			var Z_io_x float64 = 0
			for _, t := range query.TablesInQueryMap {
				Z, Z_io, err := TableScan(*t.Table, C_filter, C_b)
				if err != nil {
					return nil, fmt.Errorf("SimpleJoin TableScan error: %s", err)
				}

				// IndexScan
				C2, C2_io, err := IndexScanRead(*t.Table, *query, C_filter, C_b)
				if err != nil {
					return nil, fmt.Errorf("SimpleJoin IndexScan error: %s", err)
				}

				// Выбор min(TableScan;IndexScan)
				if C2 < Z {
					Z = C2
					Z_io = C2_io
				}

				Z_x += Z
				Z_io_x += Z_io
				var T = t.Table.T
				var condition, cErr = query.GetAllCondition(t.GetID())
				if cErr != nil {
					return nil, fmt.Errorf("SimpleJoin GetAllCondition error: %s ", cErr)
				}
				T *= condition
				T_x *= T // Если в простом запросе более одной таблицы, то декартово произведение
			}
			queryMinTime = Z_x
			queryMinTimeIO = Z_io_x
			readRowCount = T_x

		} else {
			// выбор уникальных id таблиц, участвующих во всех join //этот шаг нужен чтобы таблицы не повторялись
			var queryTablesTemp = map[string]bool{}
			for _, jsm := range query.Joins {
				for _, jm := range jsm.Join {
					queryTablesTemp[jm.TableId] = true // изощренный set - множество
				}
			}

			// выбор id таблиц, участвующих во всех join
			var queryTables []string
			for iut, _ := range queryTablesTemp {
				queryTables = append(queryTables, iut)
			}
			if len(queryTables) < 2 {
				return nil, fmt.Errorf("too few tabels for any joins (< 2)")
			}
			if TEST {
				queryTables = []string{"8500d2ce-9c4d-1001-60fc-4899dd820e4d", "2268b2fb-117b-b5c8-2fbc-366dbca7ce4b", "9fa9d612-5f6c-496a-e5bb-7459f70e8c24", "eea05159-6d22-1815-dcd4-0819261fb8af", "2e6ff751-593c-fddf-7372-ac37521da565", "69d0efeb-1f49-faa1-e622-f3aaea84d164"} // [{NATION} {SUPPLIER} {PART} {ORDERS} {LINEITEM} {PARTSUPP}]
			}
			// конструирование всех вариантов соединения таблиц n! штук
			var allJoinVariations = PermutationsOfN(len(queryTables))
			if TEST && len(TESTSEQUENCE) == len(queryTables) {
				allJoinVariations = [][]int{TESTSEQUENCE}
			}

			// проход по всем вариантам из n!
			for _, jv := range allJoinVariations {
				var Z_x float64 = 0
				var Z_io_x float64 = 0
				var T_x float64 = 1
				var X []*parser.TableInQuery // Левый аргумент соединения

				// Обработка первого аргумента соединения
				for _, i := range jv {
					// пусть X соединяется с Н по атрибуту а или (a AND b)
					// Выбор таблицы, которая будет справа в соединении Y
					var currentQueryTableId = queryTables[i]
					var tableInQuery, hasTableInQuery = query.TablesInQueryMap[currentQueryTableId]
					if !hasTableInQuery {
						return nil, fmt.Errorf("can`t find table (%s) used into join tables for query (%s)", currentQueryTableId, query.Name)
					}

					var t = tableInQuery
					var T = t.Table.T
					var Z float64 = 0
					var Z_io float64 = 0
					if len(X) == 0 {
						// AccessPlan для первой таблицы
						// TableScan
						var err error
						Z, Z_io, err = TableScan(*t.Table, C_filter, C_b) // *t.Table - реальная таблица, для которой создан псевдоним или нет, но она находится в queries.TablesInQuery
						if err != nil {
							return nil, fmt.Errorf("AccessPlan TableScan error: %s", err)
						}

						C2, C2_io, err := IndexScanRead(*t.Table, *query, C_filter, C_b)
						if err != nil {
							return nil, fmt.Errorf("AccessPlan IndexScan error: %s", err)
						}

						// Выбор min(TableScan;IndexScan)
						if C2 < Z {
							Z = C2
							Z_io = C2_io
						}
						// Фильтрация
						// Число записей в промежуточной таблице подзапроса с учетом условия селекции
						var condition, cErr = query.GetAllCondition(t.GetID())
						if cErr != nil {
							return nil, fmt.Errorf("GetAllCondition error: %s ", cErr)
						}
						T *= condition

					} else {
						// JoinPlan для таблиц 2:n
						// Оценка подзапроса в рамках join
						var AttrJoin, P, I_x, err = query.GetJoinAttr(X, *t, T_x)
						if err != nil {
							return nil, fmt.Errorf("Calculate I for Join error: %s. ", err)
						}
						if AttrJoin == nil {
							// Декартово произведение
							// Оценка Y
							C, C_io, err := TableScan(*t.Table, C_filter, C_b)
							if err != nil {
								return nil, fmt.Errorf("Evaluation of a subquery TableScan error: %s. ", err)
							}
							// Оценка соединения
							Z = C + C_join
							Z_io = C_io + C_join_io
						} else {
							// Соединение по индексу
							// Оценка Y
							C, C_io, _, err := JoinPlanRead(*t.Table, AttrJoin, T_x, C_filter, C_b)
							if err != nil {
								return nil, fmt.Errorf("Evaluation of a subquery IndexScan error: %s. ", err)
							}
							// Оценка соединения
							Z = C
							Z_io = C_io

							T *= P
						}
						// Определение числа записей в Y
						// Определение p для Y
						var condition, cErr = query.GetAllCondition(t.GetID())
						if cErr != nil {
							return nil, fmt.Errorf("JoinPlan GetAllCondition error: %s. ", cErr)
						}

						T *= condition

						// Определение числа записей в соединении
						if AttrJoin == nil {
							// Число записей при декартовом произведении
							T = math.Ceil(T_x * T)
						} else {
							// Число записей при соединении по условию
							T = math.Ceil((T_x * T) / (math.Max(I_x, math.Min(T, AttrJoin.I)))) // I_x - мощность атрибута соединения (a) в X;
							// T_x - число записей в X;
							// I - мощность атрибута соединения (а) в Y;
							// T - число записей в Y
						}
					}

					// Оценка соединения
					Z_x += Z
					Z_io_x += Z_io
					T_x = T

					// Конец итерации
					X = append(X, t)
				}

				if queryMinTime == -1 || Z_x < queryMinTime {
					queryMinTime = Z_x
					queryMinTimeIO = Z_io_x
					readRowCount = T_x
					// fmt.Println(query.Name, Z_x, Z_io_x, T_x, X)
				}
			}

		}
		var resultRowCount, resultRowSize, orderTime, err = getResultRowCountAndSize(query, readRowCount, n_proc, C_filter)
		if err != nil {
			return nil, err
		}
		queryMinTime += orderTime
		queriesMinTime = append(queriesMinTime, parser.QueriesMinTime{Query: query, Time: queryMinTime, TimeIO: queryMinTimeIO, OrderTime: orderTime, RowsCount: resultRowCount, RowSize: resultRowSize}) // запись в массив минимального времени выполнение очередного запроса
	}
	fmt.Printf("%v \n", parser.QueriesMinTimes(queriesMinTime))
	return queriesMinTime, nil
}

//func CalculateOnlineTransaction(params *parser.DataModel, queriesTimes parser.QueriesMinTimes) (parser.TransactionResult, error) {
//	return parser.TransactionResult(nil), nil
//}

//func SetRequestIncrementValues() {
//
//}
//
//func SetNodeIncrementValues(node parser.Node) parser.Node {
//
//}

func SetField(obj interface{}, fieldName string, value interface{}) error {
	val := reflect.ValueOf(obj)
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("object must be a pointer (can't set field of not pointer)")
	}
	val = val.Elem()

	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("field is not valid")
	}
	if !field.CanSet() {
		return fmt.Errorf("field can't be set")
	}
	newVal := reflect.ValueOf(value)
	fieldType := field.Type()
	newType := newVal.Type()
	if fieldType != newType {
		if !newType.ConvertibleTo(fieldType) {
			return fmt.Errorf("can't set new val: expected %s, got %s", fieldType, newType)
		}
		newVal = newVal.Convert(fieldType)
	}
	field.Set(newVal)
	return nil
}

func EvaluateRequest(inputParams parser.InputParams, Increment parser.IncrementValueMap, sn int) (parser.RequestsResultsInc, error) {
	// по итерациям
	var result parser.RequestsResultsInc

	// var Increment = parser.IncrementValueMap{"ea2036a5-f5e8-cfe0-57a2-1c438fae47c0_NodeCount":50, "abc47bdf-0620-cd6b-07a5-89b59201549e_Frequency":10.0, "c951ff22-2f1d-5732-bc28-829eac37a377_Mode":"online"}
	for _, r := range inputParams.Request {
		var request = *r                  // копия объекта r
		var node = *request.Database.Node //Cluster node -> NodeCount  DiskCount  Proc  Disk Name
		var database = request.Database
		var nodeClient = *request.Node //PC or Cluster -> NodeCount Name

		var IncrementForPrint = make(parser.IncrementValueMap)
		var QueriesForPrint = make(parser.DatabaseValueMap)
		// выставляем инкрементные значения
		for k, v := range Increment {
			var id, field, err = parser.ParseIncrementID(k)
			if err != nil {
				return nil, err
			}
			var obj interface{}
			var name = ""
			switch id {
			case request.GetID():
				obj = &request
				name = request.Name
			case request.Database.NodeID:
				obj = &node
				name = node.Name
			case request.NodeID:
				obj = &nodeClient
				name = nodeClient.Name
			default:
				continue

			}
			err = SetField(obj, field, v)
			if err != nil {
				return nil, err
			}
			IncrementForPrint[fmt.Sprintf("%s_%s", name, field)] = v
		}
		var frequency = request.Frequency
		var mode = request.Mode

		var C_filter float64 = GLOBALVARS.K / helper.GigagertzToGertz(node.Proc)
		var C_b float64 = GLOBALVARS.D / helper.MegabyteToByte(node.Disk)

		if frequency == 0 {
			return parser.RequestsResultsInc(nil), nil // TODO вывод с нулевым временем
		}

		var resultByQuery parser.QueriesMinTimes
		q, ok := ALREADY_CALCULATED_DATA_MODEL[getQueryTimesCacheID(database.DataModel, node)]
		if !ok {
			var err error
			resultByQuery, err = EvaluateQueries(database.DataModel, C_filter, C_b, node.DiskCount)
			if err != nil {
				return nil, err
			}
			ALREADY_CALCULATED_DATA_MODEL[getQueryTimesCacheID(database.DataModel, node)] = resultByQuery
			fmt.Println("QUERY_CALCULATE")
		} else {
			resultByQuery = q
		}
		QueriesForPrint[database.Name] = resultByQuery.ToQueryValueMap()
		var N_proc = node.NodeCount //число машин в кластере, на котором размещена БД и транзакция
		var N_disc = node.DiskCount //число дисков в кластере, на котором размещена БД и транзакция
		var QueriesSumTime float64 = 0
		var QueriesSumTimeIO float64 = 0
		var TransactionSize float64 = 0 //размер транзакции в байтах
		for _, q := range request.Transaction.Queries {
			for _, rq := range resultByQuery {
				if rq.Query.GetID() == q.GetID() {
					QueriesSumTime += rq.Time * q.Count
					QueriesSumTimeIO += rq.TimeIO * q.Count
					if !q.Subquery { // поздапросы не влияют на объем
						TransactionSize += rq.RowsCount * rq.RowSize
					}
					break
				}
			}
		}

		//передача по сети
		var NetworkSpeed float64 = -1
		if nodeClient.GetID() != node.GetID() && mode != OfflineTransactionType { //кластер обращается не сам к себе
			// Ищем сеть, связывающую request.Node и request.Database.Node
			for _, net := range inputParams.Network {
				var findClient = false
				var findClaster = false
				for _, nodeID := range net.NodesID {
					if nodeID == nodeClient.GetID() {
						findClient = true
					}
					if nodeID == node.GetID() {
						findClaster = true
					}
				}
				if findClient && findClaster {
					NetworkSpeed = net.Speed
					if NetworkSpeed == 0 {
						return nil, fmt.Errorf("Network %s has speed 0 Mbit/sec. ", net.Name)
					}
					break
				}
			}
			if NetworkSpeed == -1 {
				return nil, fmt.Errorf("Can`t request from node %s to %s without network. ", nodeClient.Name, node.Name)
			}
		}
		var TransactionTime float64 = 0
		var NetworkCharge float64 = 0
		var T_network float64 = 0
		var DiscCharge float64 = 0
		var ProcCharge float64 = 0
		var T_network_time float64 = 0
		// расчет транзакции Online
		if mode == OnlineTransactionType {
			var intension = helper.HourToSecond(frequency) * nodeClient.NodeCount //число клиентов
			DiscCharge = intension * QueriesSumTimeIO / N_disc
			ProcCharge = intension * QueriesSumTime / N_proc // TODO нужно ли считать время для всех транзакций?
			for _, q := range request.Transaction.Queries {
				for _, rq := range resultByQuery {
					if rq.Query.GetID() == q.GetID() {
						TransactionTime += q.Count * (rq.Time/(N_proc*(1-ProcCharge)) + rq.TimeIO/(N_disc*(1-DiscCharge)))
						break
					}
				}
			}

			T_network_time = TransactionSize / helper.MbitToByte(NetworkSpeed)
			NetworkCharge = intension * T_network_time
			T_network = T_network_time / (1 - NetworkCharge)

			TransactionTime += T_network
		}

		if mode == OfflineTransactionType {
			NetworkSpeed = 0 // сеть для offline не играет роли
			var n = frequency
			var P_proc = (QueriesSumTime * n / N_proc) / ((QueriesSumTime * n / N_proc) + (QueriesSumTimeIO * n / N_disc))
			var P_disc = 1 - P_proc
			var K_proc = P_proc * (n - 1) / N_proc
			var K_disc = P_disc * (n - 1) / N_disc
			for _, q := range request.Transaction.Queries {
				for _, rq := range resultByQuery {
					if rq.Query.GetID() == q.GetID() {
						TransactionTime += q.Count * (rq.Time*((1/N_proc)+K_proc) + rq.TimeIO*((1/N_disc)+K_disc))
						break
					}
				}
			}
		}

		fmt.Println(QueriesForPrint)

		result = append(result, parser.RequestResultInc{
			sn,
			parser.RequestResult{TransactionResult: parser.TransactionResult{Transaction: request.Transaction.Name, Time: TransactionTime, DiscCharge: DiscCharge, ProcCharge: ProcCharge, Size: TransactionSize}, NetworkCharge: NetworkCharge, NetworkTime: T_network, NetworkSpeed: helper.MbitToByte(NetworkSpeed)},
			IncrementForPrint,
			QueriesForPrint,
		}) // запись в массив минимального времени выполнение очередного запроса

	}
	return result, nil
}
