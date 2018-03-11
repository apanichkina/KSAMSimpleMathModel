// realization of https://cwiki.apache.org/confluence/display/Hive/Cost-based+optimization+in+Hive
// cost based optimisation funcs
package hive

import "math"

// TableScanCost
//
// CPU Usage = 0.
//
// IO Usage = Hr * T(R) * Tsz.
func TableScanCost(t Table) Cost {
	return NewCost(
		0,
		Hr*t.Tr*t.Tsz(),
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
	tables ...Table,
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
	}

	return NewCost(
		sortCost+mergeCost,
		Lw*workingDataSize+Lr*workingDataSize+NEt*workingDataSize,
	)
}

// MapJoinCost
//
// CPU Usage = HashTable Construction cost + Cost of Join
// = (T(R2) + ...+ T(Rm)) + (T(R1) + T(R2) + ...+ T(Rm)) * CPUc nano seconds
//
// IO Usage = Cost of transferring small tables to Join Operator Node * Parallelization of the join
// = NEt * (T(R2) * Tsz2 + ... + T(Rm) * Tszm) * number of mappers
//
// R1, R2... Rm is the relations involved in join and R1 is the big table that will be streamed.
// Tsz2... Tszm are the average size of tuple in relations R1, R2...Rm.
func MapJoinCost(
	tables ...Table,
) Cost {
	if len(tables) == 0 {
		return NewCost(0, 0)
	}
	ind, biggest := 0, tables[0]
	for i, t := range tables {
		if t.Tr > biggest.Tr {
			biggest = t
			ind = i
		}
	}

	// CPU
	var (
		hashTableConstructionCost float64
		joinCost                  float64
	)

	// IO
	var transferingTablesCost float64

	for i, v := range tables {
		joinCost += v.Tr

		if ind == i {
			continue
		}

		hashTableConstructionCost += v.Tr
		transferingTablesCost += v.Tr * v.Tsz()
	}

	return NewCost(
		hashTableConstructionCost+joinCost*CPUc,
		NEt*transferingTablesCost*NumberOfMappers,
	)
}

// BucketMapJoinCost
//
// CPU Usage = Hash Table Construction cost
// + Cost of Join
// = (T(R2) + ...+ T(Rm)) * CPUc
// + (T(R1) + T(R2) + ...+ T(Rm)) * CPUc nano seconds
//
// IO Usage = Cost of transferring small tables to Join Operator Node * Parallelization of the join
// = NEt * (T(R2) * Tsz2 + ... + T(Rm) * Tszm) * number of mappers
//
// R1, R2... Rm is the relations involved in join and R1 is the big table that will be streamed.
// Tsz2... Tszm are the average size of tuple in relations R1, R2...Rm.
func BucketMapJoinCost(
	tables ...Table,
) Cost {
	if len(tables) == 0 {
		return NewCost(0, 0)
	}
	ind, biggest := 0, tables[0]
	for i, t := range tables {
		if t.Tr > biggest.Tr {
			biggest = t
			ind = i
		}
	}

	// CPU
	var (
		hashTableConstructionCost float64
		joinCost                  float64
	)

	// IO
	var transferingTablesCost float64

	for i, v := range tables {
		joinCost += v.Tr

		if ind == i {
			continue
		}

		hashTableConstructionCost += v.Tr
		transferingTablesCost += v.Tr * v.Tsz()
	}

	return NewCost(
		hashTableConstructionCost*CPUc+joinCost*CPUc,
		NEt*transferingTablesCost*NumberOfMappers,
	)
}

// SMBJoinCost
//
// CPU Usage = Cost of Join
// = (T(R1) + T(R2) + ...+ T(Rm)) * CPUc nano seconds
//
// IO Usage = Cost of transferring small tables to Join * Parallelization of the join
// = NEt * (T(R2) * Tsz2 + ... + T(Rm) * Tszm) * number of mappers
//
// R1, R2... Rm is the relations involved in join.
// Tsz2... Tszm are the average size of tuple in relations R2...Rm.
func SMBJoinCost(
	tables ...Table,
) Cost {
	var workingDataSize float64
	var cpu float64

	for _, v := range tables {
		cpu += v.Tr * CPUc
		workingDataSize += v.Tr * v.Tsz()
	}

	return NewCost(
		cpu,
		NEt*workingDataSize*NumberOfMappers,
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
		Lw*t.Tr*t.Tsz()+Lr*t.Tr*t.Tsz()+NEt*t.Tr*t.Tsz(),
	)
}

// FilterCost
//
// Filter/Having
//
// CPU Usage = T(R) * CPUc nano seconds
//
// IO Usage = 0
func FilterCost(t Table) Cost {
	return NewCost(
		t.Tr*CPUc,
		0,
	)
}

// SelectCost
//
// CPU Usage = 0
//
// IO Usage = 0
func SelectCost(t Table) Cost {
	return NewCost(0, 0)
}
