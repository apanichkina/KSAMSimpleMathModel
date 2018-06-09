package main

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
)

func newf(f float64) *float64 {
	return &f
}

//select
//ap.partkey,
//asup.suppkey,
//s1.ps_availqty,
//s3.ps_supplycost,
//s2.ps_comment
//from anc_partsupp aps
//join tie_partsupp tps on aps.partsupp_sid = tps.partsupp_sid
//join anc_part ap on tps.part_sid = ap.part_sid
//join anc_supplier asup on tps.supp_sid = asup.supp_sid
//join sat_partsupp_availqty s1 on aps.partsupp_sid = s1.ps_sid
//join sat_partsupp_comment s2 on aps.partsupp_sid = s2.ps_sid
//join sat_partsupp_supplycost s3 on aps.partsupp_sid = s3.ps_sid;

func main() {
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

	fmt.Println(hive.SumCosts(
		hive.TableScanCost(anc_partsupp, 6),
		hive.FilterCost(anc_partsupp, 6),

		hive.TableScanCost(tie_partsupp, 6),
		hive.FilterCost(tie_partsupp, 6),

		hive.TableScanCost(anc_part, 6),
		hive.FilterCost(anc_part, 6),

		hive.TableScanCost(anc_supplier, 6),
		hive.FilterCost(anc_supplier, 6),

		hive.TableScanCost(sat_partsupp_availqty, 6),
		hive.FilterCost(sat_partsupp_availqty, 6),

		hive.TableScanCost(sat_partsupp_comment, 6),
		hive.FilterCost(sat_partsupp_comment, 6),

		hive.TableScanCost(sat_partsupp_supplycost, 6),
		hive.FilterCost(sat_partsupp_supplycost, 6),

		hive.CommonJoinCost(4000000, &anc_partsupp, &tie_partsupp,
			&anc_part, &anc_supplier, &sat_partsupp_availqty, &sat_partsupp_comment, &sat_partsupp_supplycost,
		),
	))
}
