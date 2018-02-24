package math

import (
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"math"
	"fmt"
)

type Str struct {
	W   string   // имя подзапроса
	X   string   // левый аргумент соединения
	Y   string   // правый аргумент соединения
	Z   float64  // оценка стоимости выполнения подзапроса
	ZIO float64  // оценка стоимости составляющей ввода-вывода подзапроса
	V   VOptions // опции
}

type VOptions struct {
	T float64            // оценка числа записей в подзапросе = T(Qi)
	B float64            // оценка числа блоков в подзапросе = B(Qi)
	I map[string]float64 // мощности атрибутов, которые участвуют в соединении
	k string             // индексируемый атрибут
}

func TableScan(Table parser.Table) (float64, float64, error) {
	if Table.Size == 0 {
		return 0.0, 0.0, fmt.Errorf("%s Table.Size cann`t be 0", Table.Name)
	}
	var T float64 = Table.T
	var B float64 = D / Table.Size
	var C_cpu float64 = T * C_filter
	var C_io float64 = B * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}

func IndexScanRead(Table parser.Table, Query parser.Query) (float64, float64, error) { //Допущение: Индекс не кластеризован! И индекс только на PK
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
	var B_ind float64 = D_ind / Table.PKAttribute.Size
	var C_cpu float64 = T * C_filter
	var C_io float64 = (math.Ceil(B_ind * p) + math.Ceil(Table.T * p)) * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}


func JoinPlanRead(Table parser.Table, Attr *parser.Attribute, N float64) (float64, float64, float64, error) { //Допущение: Индекс не кластеризован!
	var I = Attr.I
	if I == 0 {
		return 0.0, 0.0, 0.0, fmt.Errorf("%s Attr.I cann`t be 0 for join. ", Table.Name)
	}
	var T float64 = Table.T * 1 / I
	var C_cpu float64 = N * T * C_filter
	var B_ind float64 = D_ind / Attr.Size
	var C_io float64 = N * (math.Ceil(B_ind * 1 / I) + math.Ceil(Table.T * 1 / I)) * C_b
	var C = C_cpu + C_io

	return C, C_io, T, nil
}


func IndexScan(Table parser.Table, p float64, L float64) (float64, float64, float64, error) { //Допущение: Индекс не кластеризован!
	if L == 0 {
		return 0.0, 0.0, 0.0, fmt.Errorf("%s Attr.L cann`t be 0", Table.Name)
	}
	var T float64 = Table.T * p
	var B_ind float64 = D_ind / Table.L
	var C_cpu float64 = T * C_filter
	var C_io float64 = (math.Ceil(B_ind * p) + math.Ceil(Table.T * p)) * C_b
	var C = C_cpu + C_io

	return C, C_io, T, nil
}

func Evaluate(inputParams parser.InputParams) (parser.QueriesMinTimes, error) {
	params := inputParams.DataModel[0] // добавить проход по массиву
	fmt.Println("Считается концептуальная модель: ", params.Name)
	// подготовительные расчеты для входных параметров

	if len(params.Queries) < 1 {
		return nil, fmt.Errorf("can`t find any query")
	}
	var queriesMinTime parser.QueriesMinTimes // минимальное время выполнения всех запросов
	// проход по всем запоросам
	for _, query := range params.Queries {
		var queryMinTime float64 = -1 // минимальное время выполнения запроса

		fmt.Println("query", query.Name, query.GetID())

		if len(query.Joins) == 0 {
			// нет джоинов -> это простой запрос
			if len(query.TablesInQuery) == 0 {
				return nil, fmt.Errorf("too few tabels in query %s", query.Name)
			}
			var Z_x float64 = 0
			var Z_io_x float64 = 0
			for _, t := range query.TablesInQuery {
				Z, Z_io, err := TableScan(*t.Table)
				if err != nil {
					return nil, fmt.Errorf("SimpleJoin TableScan error: %s", err)
				}
				fmt.Println(t.Table.Name, "C1", Z, Z_io)

				// IndexScan
				C2, C2_io, err := IndexScanRead(*t.Table, *query)
				if err != nil {
					return nil, fmt.Errorf("AccessPlan IndexScan error: %s", err)
				}
				fmt.Println(t.Table.Name, "C2", C2, C2_io)
				// Выбор min(TableScan;IndexScan)
				if C2 < Z {
					Z = C2
					Z_io = C2_io
				}

				Z_x += Z
				Z_io_x += Z_io
			}
			queryMinTime = Z_x

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

			// конструирование всех вариантов соединения таблиц n! штук
			allJoinVariations := PermutationsOfN(len(queryTables))

			// проход по всем вариантам из n!
			fmt.Println(allJoinVariations)
			for _, jv := range allJoinVariations {

				fmt.Println("jv", jv)
				var Z_x float64 = 0
				var Z_io_x float64 = 0
				var T_x float64 = 1
				var B_x float64 = 0
				var B_x_join float64 = 0
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

					fmt.Println(parser.TableNames(X), "+", t.Table.Name)
					var T = t.Table.T
					var Z float64 = 0
					var Z_io float64 = 0
					if len(X) == 0 {
						// AccessPlan для первой таблицы
						// TableScan
						var err error
						Z, Z_io, err = TableScan(*t.Table) // *t.Table - реальная таблица, для которой создан псевдоним или нет, но она находится в queries.TablesInQuery
						if err != nil {
							return nil, fmt.Errorf("AccessPlan TableScan error: %s", err)
						}
						fmt.Println(t.Table.Name, "C1", Z, Z_io)

						C2, C2_io, err := IndexScanRead(*t.Table, *query)
						if err != nil {
							return nil, fmt.Errorf("AccessPlan IndexScan error: %s", err)
						}
						fmt.Println(t.Table.Name, "C2", C2, C2_io)
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
						// Оценка числа блоков в промежуточной таблице

						B_x = math.Ceil(T / (t.Table.L * L_b)) // TODO пересчитать с учетом длины блока в байтах

					} else {
						// JoinPlan для таблиц 2:n
						// Оценка подзапроса в рамках join
						var AttrJoin, P, I_x, err = query.GetJoinAttr(X, *t, T_x)
						if err != nil {
							return nil,fmt.Errorf("Calculate I for Join error: %s. ", err)
						}
						if AttrJoin == nil {
							// Декартово произведение
							// Оценка Y
							C, C_io, err := TableScan(*t.Table)
							if err != nil {
								return nil, fmt.Errorf("Evaluation of a subquery TableScan error: %s. ", err)
							}
							// Оценка соединения
							Z = C + C_join
							Z_io = C_io + C_join_io
						} else {
							// Соединение по индексу
							// Оценка Y
							C, C_io, _, err := JoinPlanRead(*t.Table, AttrJoin, T_x)
							if err != nil {
								return nil, fmt.Errorf("Evaluation of a subquery IndexScan error: %s. ", err)
							}
							// Оценка соединения
							Z = C
							Z_io = C_io

							T *=  P
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
							B_x = math.Ceil(T / (t.Table.L * L_b))
							T = math.Ceil(T_x * T)
						} else {
							// Число записей при соединении по условию
							T = math.Ceil((T_x * T) / (math.Max(I_x, AttrJoin.I))) // I_x - мощность атрибута соединения (a) в X;
							// T_x - число записей в X;
							// I - мощность атрибута соединения (а) в Y;
							// T - число записей в Y
							B_x = math.Ceil(T / (t.Table.L * L_b))
						}

						B_x_join = math.Ceil(T / L_join)
					}

					// Оценка соединения
					Z_x += Z
					Z_io_x += Z_io
					T_x = T
					B_x = B_x_join
					// Конец итерации
					fmt.Printf("table %s %.2f %.2f %.2f %.2f \n", t.Table.Name, Z_x, Z_io_x, T_x, B_x)
					X = append(X, t)
				}
				if queryMinTime == -1 {
					queryMinTime = Z_x
				} else {
					queryMinTime = math.Min(queryMinTime, Z_x)
				}
				fmt.Println()
			}
		}
		queriesMinTime = append(queriesMinTime, parser.QueriesMinTime{Query: query, Time: parser.FullFloat64(queryMinTime)}) // запись в массив минимального времени выполнение очередного запроса
	}
	return parser.QueriesMinTimes(queriesMinTime), nil
}
