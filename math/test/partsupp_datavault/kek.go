package partsupp_datavault

import (
	"math"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func newf(f float64) *float64 {
	return &f
}

func GetDataVault() hive.Cost {
	anc_partsupp := hive.Table{
		Tr:      4000000,
		TszTemp: newf(42),
	}

	tie_partsupp := hive.Table{
		Tr:      4000000,
		TszTemp: newf(42 + 44),
	}

	anc_part := hive.Table{
		Tr:      1000000,
		TszTemp: newf(42 + 44 + 42),
	}

	anc_supplier := hive.Table{
		Tr:      50000,
		TszTemp: newf(42 + 44 + 42),
	}

	sat_partsupp_availqty := hive.Table{
		Tr:      4000000,
		TszTemp: newf(42 + 44 + 42 + 24),
	}

	sat_partsupp_comment := hive.Table{
		Tr:      4000000,
		TszTemp: newf(42 + 44 + 42 + 24 + 36),
	}

	sat_partsupp_supplycost := hive.Table{
		Tr:      4000000,
		TszTemp: newf(42 + 44 + 42 + 24 + 36 + 24),
	}

	return hive.SumCosts(
		hive.TableScanCost(anc_partsupp, math.Min(anc_partsupp.Tr*anc_partsupp.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(anc_partsupp, math.Min(anc_partsupp.Tr*anc_partsupp.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(tie_partsupp, math.Min(tie_partsupp.Tr*tie_partsupp.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(tie_partsupp, math.Min(tie_partsupp.Tr*tie_partsupp.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(anc_part, math.Min(anc_part.Tr*anc_part.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(anc_part, math.Min(anc_part.Tr*anc_part.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(anc_supplier, math.Min(anc_supplier.Tr*anc_supplier.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(anc_supplier, math.Min(anc_supplier.Tr*anc_supplier.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(sat_partsupp_availqty, math.Min(sat_partsupp_availqty.Tr*sat_partsupp_availqty.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(sat_partsupp_availqty, math.Min(sat_partsupp_availqty.Tr*sat_partsupp_availqty.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(sat_partsupp_comment, math.Min(sat_partsupp_comment.Tr*sat_partsupp_comment.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(sat_partsupp_comment, math.Min(sat_partsupp_comment.Tr*sat_partsupp_comment.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.TableScanCost(sat_partsupp_supplycost, math.Min(sat_partsupp_supplycost.Tr*sat_partsupp_supplycost.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),
		hive.FilterCost(sat_partsupp_supplycost, math.Min(sat_partsupp_supplycost.Tr*sat_partsupp_supplycost.Tsz()/hive.BlockSize, hive.MaxNumberOfMappers)),

		hive.CommonJoinCost(4000000, &anc_partsupp, &tie_partsupp,
			&anc_part, &anc_supplier, &sat_partsupp_availqty, &sat_partsupp_comment, &sat_partsupp_supplycost,
		),
	)
}
