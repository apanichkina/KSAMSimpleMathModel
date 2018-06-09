package anchor

import (
	"math"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func kek(size float64, rowSize ...float64) hive.Cost {
	tables := []*hive.Table{}

	for _, v := range rowSize {
		tables = append(tables, &hive.Table{Tr: size, TszTemp: &v})
	}

	result := hive.Cost{}
	for _, v := range tables {
		result = result.Add(hive.FilterCost(*v, math.Min(v.Tr*v.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)))
		result = result.Add(hive.TableScanCost(*v, math.Min(v.Tr*v.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)))
	}

	return result.Add(hive.CommonJoinCost(size, tables...))
}

func GetAnchor() hive.Cost {
	return kek(29999809,
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
		64)
}
