package main

import (
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"log"
	Math "math"
)

const input = `
{
	"input":3,
	"Tables": [
		{"Id": "4", "Name": "J", "T": 10000, "L": 500, "Attributes": [
			{"Id": "41", "Name": "город", "I": 50},
			{"Id": "42", "Name": "ном_изд", "I": 10000}
		]},
		{"Id": "1", "Name": "S", "T": 10000, "L": 500, "Attributes": [
			{"Id": "11", "Name": "город", "I": 50},
			{"Id": "12", "Name": "ном_пост", "I": 10000},
			{"Id": "13", "Name": "имя", "I": 9000}
		]},
		{"Id": "2", "Name": "P", "T": 100000, "L": 500, "Attributes": [
			{"Id": "21", "Name": "название", "I": 100000},
			{"Id": "22", "Name": "ном_дет", "I": 100000},
			{"Id": "23", "Name": "цвет", "I": 20}
		]},
		{"Id": "3", "Name": "SPJ", "T": 1000000, "L": 1000, "Attributes": [
			{"Id": "31", "Name": "ном_изд", "I": 10000},
			{"Id": "32", "Name": "ном_дет", "I": 100000},
			{"Id": "33", "Name": "ном_пост", "I": 5000}
		]}
	],
	"Queries": [
		{
			"Id": "013",
			"Name": "Q13",
			"Joins": [
				{
					"Id": "101",
					"Join": [
						{"TableId": "1", "AttributeId": "12"},
						{"TableId": "3", "AttributeId": "33"}
					]
				},
				{
					"Id": "102",
					"Join": [
						{"TableId": "2", "AttributeId": "22"},
						{"TableId": "3", "AttributeId": "32"}
					]
				}
			],
			"Projections": [
				{"TableId": "1", "AttributeId": "13"}
			],
			"Conditions": [
				{"TableId": "1", "AttributeId": "11", "P": 0.02},
				{"TableId": "3", "AttributeId": "31", "P": 0.0001},
				{"TableId": "2", "AttributeId": "23", "P": 0.00001}
			]
		}
	]
}
`

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

const inputDictSameId = `
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
						"1": {"TableId": "1", "AttributeId": "12"},
						"3": {"TableId": "3", "AttributeId": "33"}
					}
				},
				"102": {
					"Id": "102",
					"Join": {
						"2": {"TableId": "2", "AttributeId": "22"},
						"3": {"TableId": "3", "AttributeId": "32"}
					}
				}
			},
			"Projections": {
				"1": {"TableId": "1", "AttributeId": "13"}
			},
			"Conditions": {
				"1": {"TableId": "1", "AttributeId": "11", "P": 0.02},
				"3": {"TableId": "3", "AttributeId": "31", "P": 0.0001},
				"2": {"TableId": "2", "AttributeId": "23", "P": 0.00001}
			}
		}
	}
}
`

func main(){


	params, err := parser.GetInputParamsFromString(input)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("%+v", params.Queries[0].Conditions[0].Table.Attributes[0])
	fmt.Println("%+v", params.Queries[0].JoinsMap["101"])
	fmt.Printf("%+v", params)
	fmt.Println("%+v", math.L)





	result, err := math.MakeMath(params)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("%+v", result)

	var query = params.Queries[0] // заменить на цикл по всем Q


	var queryTablesTemp = map[string]bool{}

	for _, jsm := range query.JoinsMap {
		for _, jm := range jsm.JoinMap {
			queryTablesTemp[jm.TableId] = true
		}
	}
	var queryTables []string
	for iut, _ := range queryTablesTemp {
		queryTables = append(queryTables, iut)
	}
	fmt.Println("%+v", queryTablesTemp)
	fmt.Println("%+v", queryTables)
	var allJoinVariations = math.PermutationsOfN(len(queryTables))
	for _, jv := range allJoinVariations {
		fmt.Println(jv)
		var Z_x float64 = 0
		var Z_io_x float64 = 0
		var T_x float64 = 1
		var B_x float64 = 0
		var B_join float64 = 0
		var isFirst = true
		var C_join float64 = 0 // как?
		var С_io_join float64 = 0
		var X []*parser.Table // Левый аргумент соединения

		for ind, i := range jv {

			var currentTableId = queryTables[i]
			var table, hasTable = params.TablesMap[currentTableId]
			if !hasTable {
				log.Fatal("cant find table used into join")
			}
			var t = *table

			fmt.Println(parser.TableIDs(X), "+", currentTableId, t.Name)
			var T = t.T
			var Z float64 = 0
			var Z_io float64 = 0
			if isFirst {
				Z, Z_io, err = math.TableScan(t)
				if err != nil {
					log.Fatal(err)
				}

				fmt.Println(t.Name, "C1", Z, Z_io)


				var condition, cErr = query.GetAllCondition(t)
				if cErr != nil {
					log.Fatal(cErr)
				}

				if condition != 1 {
					C2, C2_io, T_Q, err := math.IndexScan(t, condition)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(t.Name, "C2", C2, C2_io)
					if C2 < Z {
						Z = C2
						Z_io = C2_io
					}
					T = T_Q
				}
				B_x = Math.Ceil(T/(t.L * math.L_b)) // ??


			} else {

				var I, I_x, err = query.GetJoinI(X, t)
				if err != nil {
					log.Fatal(err)
				}
				if I == 0 {
					// нужно декартово произведение как учесть С join
					C, C_io, err := math.TableScan(t)
					if err != nil {
						log.Fatal(err)
					}
					C_join = 0 // как?
					С_io_join = 0
					Z = T_x * C + C_join
					Z_io = T_x * C_io + С_io_join
				} else {
					C, C_io, _, err := math.IndexScan(t, 1/I)
					if err != nil {
						log.Fatal(err)
					}
					Z = T_x * C
					Z_io = T_x * C_io
				}
				var condition, cErr = query.GetAllCondition(t)
				if cErr != nil {
					log.Fatal(cErr)
				}
				if condition != 1 {
					T *= condition
				}
				T = Math.Ceil((T_x * T) / (Math.Max(Math.Min(I_x, T_x), I)))

				// fmt.Println("I for", X, "and", t.Id, " ", i)
				B_x = Math.Ceil(T/(t.L * math.L_b)) // ??
				B_join = Math.Ceil(T / math.L_join)
			}

			Z_x += Z
			Z_io_x += Z_io
			T_x = T
			isFirst = false
			// конец AccessPlan
			fmt.Printf("table %.2f %s %.2f %.2f %.2f %.2f %.2f \n", ind, t.Name, Z_x, Z_io_x, T_x, B_x, B_join)
			X = append(X, table)
		}
		fmt.Println()
	}
	fmt.Println(allJoinVariations)
}
