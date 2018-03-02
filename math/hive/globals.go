package hive

var (
	CPUc float64 = 1          //nano sec
	NEt          = 150 * CPUc //nano sec
	Lw           = 4 * NEt
	Lr           = 4 * NEt
	Hw           = 10 * Lw
	Hr           = 1.5 * Lr

	NumberOfMappers float64 = 1
)
