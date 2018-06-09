package experiment

import (
	"fmt"

	"github.com/apanichkina/KSAMSimpleMathModel/math/hive_runner/data"
)

var results9 = []float64{36.43, 53.32, 67.23, 237.44, 1752.24, 17232.21}
var results1 = []float64{34.89, 39.42, 43.12, 78.87, 350.97, 3520.86}

func GetApacheExperiments() []ExperimentInterface {
	result := []ExperimentInterface{}

	queriesApache9, queriesApache1 := data.GetExperiments()
	fmt.Println(len(queriesApache9), len(queriesApache1))
	for i, v := range queriesApache9 {
		result = append(result, Experiment{v, results9[i]})
	}
	for i, v := range queriesApache1 {
		result = append(result, Experiment{v, results1[i]})
	}
	return result
}
