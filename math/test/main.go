package main

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/math/test/partsupp_datavault"
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
	fmt.Println(partsupp_datavault.GetDataVault())
}
