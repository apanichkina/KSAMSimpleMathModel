// realization of https://cwiki.apache.org/confluence/display/Hive/Cost-based+optimization+in+Hive
// cost based optimisation funcs
package hive

import (
	"fmt"
	"math"
)

// TableScanCost
//
// CPU Usage = 0.
//
// IO Usage = Hr * T(R) * Tsz.
func TableScanCost(t Table, numberOfMappers float64) Cost {
	numberOfMappers = math.Min(numberOfMappers, MaxNumberOfMappers)

	return NewCost(
		0,
		Hr*t.Tr*t.Tsz(),
		0,
		numberOfMappers,
	)
}

// CommonJoinCost
//
// CPU Usage = Sorting Cost for each of the relation
// + Merge Cost for sorted stream
// = (T(R1) * log T(R1) * CPUc + T(R2) * log T(R2) * CPUc + ... + T(Rm) * log T(Rm) * CPUc)
// + (T(R1) + T(R2) + ...+ T(Rm)) * CPUc nano seconds;
//
// IO Usage = Cost of writing intermediate result set in to local FS for shuffling
// + Cost of reading from local FS for transferring to Join operator node
// + Cost of transferring mapped output to Join operator node
// = Lw * (T(R1) * Tsz1 + T(R2) * Tsz2 + ...+ T(Rm) * Tszm)
// + Lr * (T(R1) * Tsz1 + T(R2) * Tsz2 + ...+ T(Rm) * Tszm)
// + NEt * (T(R1) * Tsz1 + T(R2) * Tsz2 + ... + T(Rm) * Tszm)
//
// R1, R2... Rm is the relations involved in join.
// Tsz1, Tsz2... Tszm are the average size of tuple in relations R1, R2...Rm.
func CommonJoinCost(
	resultSize float64,
	tables ...*Table,
) Cost {
	// cpu
	var (
		sortCost  float64
		mergeCost float64
	)

	// io
	var (
		workingDataSize float64
	)

	for _, v := range tables {
		sortCost += v.Tr * math.Log(v.Tr) * CPUc
		mergeCost += v.Tr * CPUc

		workingDataSize += v.Tr * v.Tsz()
		fmt.Printf("SIZE: %f, %f\n", v.Tr, workingDataSize)
	}

	return NewCost(
		sortCost+mergeCost,
		Hw*resultSize+Lr*workingDataSize+Lw*workingDataSize,
		(workingDataSize+resultSize)/NetSpeed,
		NumberOfReducers,
	)
}

// DistinctOrGroupByCost
//
// CPU Usage = Cost of Sorting
// + Cost of categorizing into group = (T(R) * log T(R) + T(R)) * CPUc nano seconds;
//
// IO Usage = Cost of writing intermediate result set in to local FS for shuffling
// + Cost of reading from local FS for transferring to GB reducer operator node
// + Cost of transferring data set to GB Node
// = Lw * T(R) * Tsz + Lr * T(R) * Tsz + NEt * T(R) * Tsz
func DistinctOrGroupByCost(t Table) Cost {
	return NewCost(
		(t.Tr*math.Log(t.Tr)+t.Tr)*CPUc,
		Lw*t.Tr*t.Tsz()+Lr*t.Tr*t.Tsz(),
		(t.Tr*t.Tsz())/NetSpeed,
		1,
	)
}

// FilterCost
//
// Filter/Having
//
// CPU Usage = T(R) * CPUc nano seconds
//
// IO Usage = 0
func FilterCost(t Table, numberOfMappers float64) Cost {
	numberOfMappers = math.Min(numberOfMappers, MaxNumberOfMappers)
	return NewCost(
		t.Tr*CPUc,
		0,
		0,
		numberOfMappers,
	)
}

// SelectCost
//
// CPU Usage = 0
//
// IO Usage = 0
func SelectCost(t Table) Cost {
	return NewCost(0, 0, 0, 1)
}
