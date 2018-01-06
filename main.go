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

const inputDict = `
{
	"input":3,
	"Tables": {
		"4": {"Id": "4", "Name": "J", "T": 10000, "L": 500, "Attributes": {
			"41": {"Id": "41", "Name": "город", "I": 50},
			"42": {"Id": "42", "Name": "ном_изд", "I": 10000}
		}},
		"1": {"Id": "1", "Name": "S", "T": 10000, "L": 500, "Attributes": {
			"11": {"Id": "11", "Name": "город", "I": 50},
			"12": {"Id": "12", "Name": "ном_пост", "I": 10000},
			"13": {"Id": "13", "Name": "имя", "I": 9000}
		}},
		"2": {"Id": "2", "Name": "P", "T": 100000, "L": 500, "Attributes": {
			"21": {"Id": "21", "Name": "название", "I": 100000},
			"22": {"Id": "22", "Name": "ном_дет", "I": 100000},
			"23": {"Id": "23", "Name": "цвет", "I": 20}
		}},
		"3": {"Id": "3", "Name": "SPJ", "T": 100000, "L": 1000, "Attributes": {
			"31": {"Id": "31", "Name": "ном_изд", "I": 10000},
			"32": {"Id": "32", "Name": "ном_дет", "I": 100000},
			"33": {"Id": "33", "Name": "ном_пост", "I": 5000}
		}}
	},
	"Queries": {
		"013": {
			"Id": "013",
			"Name": "Q13",
			"Joins": {
				"101": {
					"Id": "101",
					"Join": {
						"1_12": {"TableId": "1", "AttributeId": "12"},
						"3_33": {"TableId": "3", "AttributeId": "33"}
					}
				},
				"102": {
					"Id": "102",
					"Join": {
						"2_22": {"TableId": "2", "AttributeId": "22"},
						"3_32": {"TableId": "3", "AttributeId": "32"}
					}
				}
			},
			"Projections": {
				"1_13": {"TableId": "1", "AttributeId": "13"}
			},
			"Conditions": {
				"1_11": {"TableId": "1", "AttributeId": "11", "P": 0.02},
				"3_31": {"TableId": "3", "AttributeId": "31", "P": 0.0001},
				"2_23": {"TableId": "2", "AttributeId": "23", "P": 0.00001}
			}
		}
	}
}
`

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
		for _, jsm := range query.JoinsMap {
			for _, jm := range jsm.JoinMap {
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
			var X []*parser.Table // Левый аргумент соединения

			// Обработка первого аргумента соединения
			for _, i := range jv {
				// пусть X соединяется с Н по атрибуту а или (a AND b)
				// Выбор таблицы, которая будет справа в соединении Y
				var currentTableId= queryTables[i]
				var table, hasTable= params.TablesMap[currentTableId]
				if !hasTable {
					log.Fatal("can`t find table used into join")
				}
				var t= *table

				fmt.Println(parser.TableIDs(X), "+", currentTableId, t.Name)
				var T= t.T
				var Z float64 = 0
				var Z_io float64 = 0
				if len(X) == 0 {
					// AccessPlan для первой таблицы
					// TableScan
					Z, Z_io, err = math.TableScan(t)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(t.Name, "C1", Z, Z_io)

					// Опеделение есть ли условие для использования IndexScan
					var condition, cErr= query.GetAllCondition(t)
					if cErr != nil {
						log.Fatal(cErr)
					}
					if condition != 1 {
						// IndexScan
						C2, C2_io, T_Q, err := math.IndexScan(t, condition)
						if err != nil {
							log.Fatal(err)
						}
						fmt.Println(t.Name, "C2", C2, C2_io)
						// Выбор min(TableScan;IndexScan)
						if C2 < Z {
							Z = C2
							Z_io = C2_io
						}
						// Число записей в промежуточной таблице подзапроса с учетом условия селекции
						T = T_Q
					}
					// Оценка числа блоков в промежуточной таблице
					B_x = Math.Ceil(T / (t.L * math.L_b)) // ??

				} else {
					// JoinPlan для таблиц 2:n
					// Оценка подзапроса в рамках join
					var I, I_x, err= query.GetJoinI(X, t)
					if err != nil {
						log.Fatal(err)
					}
					if I == 0 {
						// Декартово произведение
						// Оценка Y
						C, C_io, err := math.TableScan(t)
						if err != nil {
							log.Fatal(err)
						}
						// Оценка соединения
						Z = T_x * C + math.C_join
						Z_io = T_x * C_io + math.C_join_io
					} else {
						// Соединение по индексу
						// Оценка Y
						C, C_io, _, err := math.IndexScan(t, 1 / I) //  ??
						if err != nil {
							log.Fatal(err)
						}
						// Оценка соединения
						Z = T_x * C
						Z_io = T_x * C_io
					}
					// Определение числа записей в Y
					// Определение p для Y
					var condition, cErr= query.GetAllCondition(t)
					if cErr != nil {
						log.Fatal(cErr)
					}
					if condition != 1 {
						T *= condition
					}

					// Определение числа записей в соединении
					if I == 0 {
						// Число записей при декартовом произведении
						B_x = Math.Ceil(T / (t.L * math.L_b))
						T = Math.Ceil(T_x * T)
					} else {
						// Число записей при соединении по условию
						T = Math.Ceil((T_x * T) / (Math.Max(Math.Min(I_x, T_x), I))) // I_x - мощность атрибута соединения (a) в X;
																						// T_x - число записей в X;
																						// I - мощность атрибута соединения (а) в Y;
																						// T - число записей в Y
						B_x = Math.Ceil(T / (t.L * math.L_b))
					}

					B_x_join = Math.Ceil(T / math.L_join)
				}

				// Оценка соединения
				Z_x += Z
				Z_io_x += Z_io
				T_x = T
				// Конец итерации
				fmt.Printf("table %s %.2f %.2f %.2f %.2f %.2f \n", t.Name, Z_x, Z_io_x, T_x, B_x, B_x_join)
				X = append(X, table)
			}
			fmt.Println()
		}
	}
	// расчет технических характеристик
}
