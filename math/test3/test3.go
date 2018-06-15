package main

import (
	"fmt"

	"math"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func main() {
	//tr := 299998090.0
	////tr := 400000.0
	//size := float64(50)

	tr := 750000.0
	size := float64(160)

	mappers := math.Min(tr*size/hive.BlockSize, hive.MaxNumberOfMappers)

	for i := 0.0; i < 60; i++ {
		realTR := tr * (1.0 + i*0.1)
		lineitem := hive.Table{
			Tr:      realTR,
			TszTemp: &size,
		}

		result := hive.Cost{}

		result = result.Add(hive.TableScanCost(lineitem, mappers))
		result = result.Add(hive.FilterCost(lineitem, mappers))

		result.IO += tr * size * hive.Hw / mappers

		fmt.Println(result)
	}
}
