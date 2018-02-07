package math

import (
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
	"math"
	"fmt"
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

func TableScan(Table parser.Table) (float64, float64, error) {
	if Table.L == 0 {
		return 0.0, 0.0, fmt.Errorf("%s Table.L cann`t be 0", Table.Name)
	}
	var T float64 = Table.T
	var B float64 = Table.T / Table.L
	var C_cpu float64 = T * C_filter
	var C_io float64 = B * C_b
	var C = C_cpu + C_io

	return C, C_io, nil
}

func IndexScan(Table parser.Table, p float64, L float64) (float64, float64, float64, error) { //Допущение: Индекс не кластеризован!
	if L == 0 {
		return 0.0, 0.0, 0.0, fmt.Errorf("%s Attr.L cann`t be 0", Table.Name)
	}
	var T float64 = Table.T * p
	var B_ind float64 = Table.T / L
	var C_cpu float64 = T * C_filter
	var C_io float64 = (math.Ceil(B_ind * p) + math.Ceil(Table.T * p)) * C_b
	var C = C_cpu + C_io

	return C, C_io, T, nil
}
