package hive

var (
	CPUc float64 = 1          //nano sec
	NEt          = 200 * CPUc //nano sec
	Lw           = 1 * NEt
	Lr           = 1 * NEt
	Hw           = 10 * Lw
	Hr           = 1.5 * Lr

	MaxNumberOfMappers float64 = 70

	BlockSize = 64.0 * 1024.0 * 1024.0

	NumberOfReducers float64 = 16

	NetSpeed = 50.0 * 1024 * 1024 * 1024 / 8
	//MaxNumberOfMappers float64 = 300
	//NumberOfReducers   float64 = 10
)
