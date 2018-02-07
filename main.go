package main

import (
	"flag"
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"github.com/gocarina/gocsv"
	"log"
	Math "math"
	"os"
)

func checkError(message string, err error) {
	if err != nil {
		var fullError = parser.Errors{{Message: fmt.Sprint(message, err)}}
		var err1 = printToCsv("data/result.csv", fullError)
		if err1 != nil {
			log.Fatal(message, err1)
		}
		log.Fatal(message, err)
	}
}

var fileInput = flag.String("in", "./data/true_input.json", "in - input model file")

func printToCsv(filename string, output interface{}) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open %q to write: %s", filename, err)
	}
	defer f.Close()

	err = gocsv.MarshalFile(output, f) // Use this to save the CSV back to the file
	if err != nil {
		return fmt.Errorf("can't write to %q csv: %s", filename, err)
	}
	return nil
}

func main() {
	flag.Parse()

	// парсинг входного json
	inputparams, err := parser.GetInputParamsFromFile(*fileInput)
	checkError("", err)
	fmt.Println("Считается модель: ", inputparams.Name)
	var params = inputparams.DataModel[0] // добавить проход по массиву
	fmt.Println("Считается концептуальная модель: ", params.Name)

	if len(params.Queries) < 1 {
		checkError("Validate error. ", fmt.Errorf("can`t find any query"))
	}
	var queriesMinTime parser.QueriesMinTimes // минимальное время выполнения всех запросов
	// проход по всем запоросам
	for _, query := range params.Queries {
		var queryMinTime float64 = -1 // минимальное время выполнения запроса

		fmt.Println("query", query.Name, query.GetID())

		if len(query.Joins) == 0 {
			// нет джоинов -> это простой запрос
			if len(query.TablesInQuery) == 0 {
				checkError("SimpleJoin error. ", fmt.Errorf("too few tabels in query %s", query.Name))
			}
			var Z_x float64 = 0
			var Z_io_x float64 = 0
			for _, t := range query.TablesInQuery {
				var Z float64 = 0
				var Z_io float64 = 0
				Z, Z_io, err = math.TableScan(*t.Table)
				checkError("SimpleJoin TableScan error. ", err)
				fmt.Println(t.Table.Name, "C1", Z, Z_io)

				var condition, L, cErr = query.GetAllCondition(t.GetID())
				checkError("SimpleJoin GetAllCondition error. ", cErr)
				if condition != 1 {
					// IndexScan
					C2, C2_io, _, err := math.IndexScan(*t.Table, condition, L)
					checkError("AccessPlan IndexScan error. ", err)
					fmt.Println(t.Table.Name, "C2", C2, C2_io)
					// Выбор min(TableScan;IndexScan)
					if C2 < Z {
						Z = C2
						Z_io = C2_io
					}

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
				checkError("Join error. ", fmt.Errorf("too few tabels for any joins (< 2)"))
			}

			// конструирование всех вариантов соединения таблиц n! штук
			var allJoinVariations = math.PermutationsOfN(len(queryTables))

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
						checkError("Search Table. ", fmt.Errorf("can`t find table (%s) used into join tables for query (%s)", currentQueryTableId, query.Name))
					}

					var t = tableInQuery

					fmt.Println(parser.TableNames(X), "+", t.Table.Name)
					var T = t.Table.T
					var Z float64 = 0
					var Z_io float64 = 0
					if len(X) == 0 {
						// AccessPlan для первой таблицы
						// TableScan
						Z, Z_io, err = math.TableScan(*t.Table)
						checkError("AccessPlan TableScan error. ", err)
						fmt.Println(t.Table.Name, "C1", Z, Z_io)

						// Опеделение есть ли условие для использования IndexScan
						var condition, L, cErr = query.GetAllCondition(t.GetID())
						checkError("AccessPlan GetAllCondition error. ", cErr)
						if condition != 1 {
							// IndexScan
							C2, C2_io, T_Q, err := math.IndexScan(*t.Table, condition, L)
							checkError("AccessPlan IndexScan error. ", err)
							fmt.Println(t.Table.Name, "C2", C2, C2_io)
							// Выбор min(TableScan;IndexScan)
							if C2 < Z {
								Z = C2
								Z_io = C2_io
							}
							// Число записей в промежуточной таблице подзапроса с учетом условия селекции
							T = T_Q
						}
						// Оценка числа блоков в промежуточной таблице

						B_x = Math.Ceil(T / (t.Table.L * math.L_b)) // ??

					} else {
						// JoinPlan для таблиц 2:n
						// Оценка подзапроса в рамках join
						var I, I_x, err = query.GetJoinI(X, *t)
						checkError("Calculate I for Join error. ", err)
						if I == 0 {
							// Декартово произведение
							// Оценка Y
							C, C_io, err := math.TableScan(*t.Table)
							checkError("Evaluation of a subquery TableScan error. ", err)
							// Оценка соединения
							Z = T_x * C + math.C_join
							Z_io = T_x * C_io + math.C_join_io
						} else {
							// Соединение по индексу
							// Оценка Y
							C, C_io, _, err := math.IndexScan(*t.Table, 1 / I, math.L_ind) //  TODO как считать L ind
							checkError("Evaluation of a subquery IndexScan error. ", err)
							// Оценка соединения
							Z = T_x * C
							Z_io = T_x * C_io
						}
						// Определение числа записей в Y
						// Определение p для Y
						var condition, _, cErr = query.GetAllCondition(t.GetID())
						checkError("JoinPlan GetAllCondition error. ", cErr)
						if condition != 1 {
							T *= condition
						}

						// Определение числа записей в соединении
						if I == 0 {
							// Число записей при декартовом произведении
							B_x = Math.Ceil(T / (t.Table.L * math.L_b))
							T = Math.Ceil(T_x * T)
						} else {
							// Число записей при соединении по условию
							T = Math.Ceil((T_x * T) / (Math.Max(Math.Min(I_x, T_x), I))) // I_x - мощность атрибута соединения (a) в X;
							// T_x - число записей в X;
							// I - мощность атрибута соединения (а) в Y;
							// T - число записей в Y
							B_x = Math.Ceil(T / (t.Table.L * math.L_b))
						}

						B_x_join = Math.Ceil(T / math.L_join)
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
					queryMinTime = Math.Min(queryMinTime, Z_x)
				}
				fmt.Println()
			}
		}
		queriesMinTime = append(queriesMinTime, parser.QueriesMinTime{Query: query, Time: parser.FullFloat64(queryMinTime)}) // запись в массив минимального времени выполнение очередного запроса
	}
	fmt.Print(parser.QueriesMinTimes(queriesMinTime))
	// генерация csv
	err = printToCsv("data/result.csv", parser.QueriesMinTimes(queriesMinTime))
	checkError("", err)
}
