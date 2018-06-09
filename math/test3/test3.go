package main

import (
	"fmt"

	"math"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func main() {
	tr := 29999809.0
	size := float64(150)

	mappers := math.Min(tr*size/hive.BlockSize, hive.MaxNumberOfMappers)

	lineitem := hive.Table{
		//Tr:      29999809,
		Tr:      tr,
		TszTemp: &size,
	}
	result := hive.Cost{}

	result = result.Add(hive.TableScanCost(lineitem, mappers))
	result = result.Add(hive.FilterCost(lineitem, mappers))

	result.IO += tr * size * hive.Hw / mappers

	fmt.Println(result)
}
