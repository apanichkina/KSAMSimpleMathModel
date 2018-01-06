package main

import (
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"log"
	Math "math"
	"flag"
)

var fileInput = flag.String("in", "./data/input.json", "in - input model file")
func main(){
	flag.Parse()

	// парсинг входного json
	params, err := parser.GetInputParamsFromFile(*fileInput)
	if err != nil {
		log.Fatal(err)
	}
	if len(params.Queries) < 1 {
		log.Fatal("can`t find any query")
	}
	// проход по всем запоросам
	for _, query := range params.Queries {
		fmt.Println("query", query.Name, query.GetID())

		// выбор уникальных id таблиц, участвующих во всех join //этот шаг нужен чтобы таблицы не повторялись
		var queryTablesTemp= map[string]bool{}
		for _, jsm := range query.Joins {
			for _, jm := range jsm.Join {
				queryTablesTemp[jm.TableId] = true
			}
		}

		// выбор id таблиц, участвующих во всех join
		var queryTables []string
		for iut, _ := range queryTablesTemp {
			queryTables = append(queryTables, iut)
		}
		if len(queryTables) < 2 {
			log.Fatal("too few tabels for any joins (< 2)")
		}

		// конструирование всех вариантов соединения таблиц n! штук
		var allJoinVariations= math.PermutationsOfN(len(queryTables))

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
				var currentQueryTableId= queryTables[i]
				var tableInQuery, hasTableInQuery= query.TablesInQueryMap[currentQueryTableId]
				if !hasTableInQuery {
					log.Fatalf("can`t find table (%s) used into join tables for query (%s)", currentQueryTableId, query.Name)
				}

				var t = tableInQuery

				fmt.Println(parser.TableIDs(X), "+", t.Table.GetID(), t.Table.Name)
				var T = t.Table.T
				var Z float64 = 0
				var Z_io float64 = 0
				if len(X) == 0 {
					// AccessPlan для первой таблицы
					// TableScan
					Z, Z_io, err = math.TableScan(*t.Table)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(t.Table.Name, "C1", Z, Z_io)

					// Опеделение есть ли условие для использования IndexScan
					var condition, cErr= query.GetAllCondition(t.GetID())
					if cErr != nil {
						log.Fatal(cErr)
					}
					if condition != 1 {
						// IndexScan
						C2, C2_io, T_Q, err := math.IndexScan(*t.Table, condition)
						if err != nil {
							log.Fatal(err)
						}
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
					var I, I_x, err= query.GetJoinI(X, *t)
					if err != nil {
						log.Fatal(err)
					}
					if I == 0 {
						// Декартово произведение
						// Оценка Y
						C, C_io, err := math.TableScan(*t.Table)
						if err != nil {
							log.Fatal(err)
						}
						// Оценка соединения
						Z = T_x * C + math.C_join
						Z_io = T_x * C_io + math.C_join_io
					} else {
						// Соединение по индексу
						// Оценка Y
						C, C_io, _, err := math.IndexScan(*t.Table, 1 / I) //  ??
						if err != nil {
							log.Fatal(err)
						}
						// Оценка соединения
						Z = T_x * C
						Z_io = T_x * C_io
					}
					// Определение числа записей в Y
					// Определение p для Y
					var condition, cErr= query.GetAllCondition(t.GetID())
					if cErr != nil {
						log.Fatal(cErr)
					}
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
				// Конец итерации
				fmt.Printf("table %s %.2f %.2f %.2f %.2f %.2f \n", t.Table.Name, Z_x, Z_io_x, T_x, B_x, B_x_join)
				X = append(X, t)
			}
			fmt.Println()
		}
	}
	// расчет технических характеристик
}
