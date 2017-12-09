package main

import (
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/math"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"log"
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
		{"Id": "3", "Name": "SPJ", "T": 100000, "L": 1000, "Attributes": [
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


func main(){
	fmt.Println(math.PermutationsOfN(8))

	params, err := parser.GetInputParamsFromString(input)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("%+v", params.Queries[0].Conditions[0].Table.Attributes[0])
	fmt.Println("%+v", params.Queries[0].ConditionsMap["1_11"])
	fmt.Printf("%+v", params)
	fmt.Printf("%+v", math.L)



	result, err := math.MakeMath(params)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("%+v", result)

	//math.GetMax()
	//math.GetMax(3,2,11)
	//math.GetMax(1,2,3)

	dict := map[int]string{}

	dict[0] = "zero"
	var query = params.Queries[0]


	var queryTablesTemp = map[string]bool{}

	for _, js := range query.Joins {
		for _, j := range js.Join {
			var t = *j.Table
			Z, Z_io, err := math.TableScan(t)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(t.Name, "C1", Z, Z_io)
			for _, v := range query.Conditions {
				if v.Table == j.Table {
					C2, C2_io, err := math.IndexScan(t, v.P)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(t.Name, "C2", C2, C2_io)
					if C2 < Z {
						Z = C2
						Z_io = C2_io
					}
					break
				}
			}
			fmt.Println(t.Name, "Best", Z, Z_io)
		}
	}


	for _, jsm := range query.JoinsMap {
		for _, jm := range jsm.JoinMap {
			queryTablesTemp[jm.TableId] = true
		}
	}
	var queryTables = []string{}
	for iut, _ := range queryTablesTemp {
		queryTables = append(queryTables, iut)
	}
	fmt.Println("%+v", queryTablesTemp)
	fmt.Println("%+v", queryTables)
	//C1,_, err := math.TableScan(*params.Tables[2])
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//fmt.Println(C1)

	//v, ok := dict[0]
	//
	//for k,v := range dict {
	//
	//}
}
