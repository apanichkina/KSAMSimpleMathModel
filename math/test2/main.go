package main

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func kek(mappers float64, size float64, rowSize ...float64) hive.Cost {
	tables := []*hive.Table{}

	for _, v := range rowSize {
		tables = append(tables, &hive.Table{Tr: size, TszTemp: &v})
	}

	result := hive.Cost{}
	for _, v := range tables {
		result = result.Add(hive.FilterCost(*v, mappers))
		result = result.Add(hive.TableScanCost(*v, mappers))
	}
	return result.Add(hive.CommonJoinCost(size, tables...))
}

func main() {
	fmt.Println(kek(12, 29999809,
		42,
		36,
		24,
		24,
		24,
		20,
		25,
		20,
		24,
		20,
		24,
		36,
		36,
		24,
		44,
		44,
		42,
		42,
		64,
		64))
}
