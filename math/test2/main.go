package main

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
	"github.com/apanichkina/KSAMSimpleMathModel/math/test2/anchor"
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
	for i := 0.0; i < 60; i++ {
		fmt.Println(anchor.GetCustomer(750000 * (1 + i*0.01)))
	}
}
