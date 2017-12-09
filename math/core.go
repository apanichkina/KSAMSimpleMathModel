package math

import (
	"fmt"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"math"
)

type Str struct {
	W   string   // имя подзапроса
	X   string   // левый аргумент соединения
	Y   string   // правый аргумент соединения
	Z   float64  // оценка стоимости выполнения подзапроса
	ZIO float64  // оценка стоимости составляющей ввода-вывода подзапроса
	V   VOptions // опции
}

type VOptions struct {
	T float64            // оценка числа записей в подзапросе = T(Qi)
	B float64            // оценка числа блоков в подзапросе = B(Qi)
	I map[string]float64 // мощности атрибутов, которые участвуют в соединении
	k string             // индексируемый атрибут
}

func AccessPlan(Table parser.Table, Q parser.Query, Str *Str) error {
	if Table.Id == "" || Q.Id == "" {
		return fmt.Errorf("can't use empty query: %+v or table: %+v", Q, Table)
	}

	// result = Table.T * C_filter + Table.T * C_b / Table.L

	return nil
}

func TableScan(Table parser.Table) (float64, float64, error) {
	var C_cpu float64 = Table.T * C_filter
	var C_io float64 = Table.T * C_b / Table.L
	var C = C_cpu + C_io

	return C, C_io, nil
}

func IndexScan(Table parser.Table, p float64) (float64, float64, error) {
	var C_cpu float64 = Table.T * C_filter * p
	var C_io float64 = (math.Ceil(Table.T*p/Table.L) + math.Ceil(Table.T*p)) * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}

func MakeMath(a parser.InputParams) (int, error) {
	if a.Input < 0 {
		return 0, fmt.Errorf("can't use negative input: %d", a.Input)
	}

	return a.Input + 10, nil
}

func GetMax(arr ...float64) {
	if len(arr) == 0 {
		return
	}

	tempMax := arr[0]
	for _, v := range arr[1:] {
		if v > tempMax {
			tempMax = v
		}
	}
	fmt.Println(tempMax)
}
