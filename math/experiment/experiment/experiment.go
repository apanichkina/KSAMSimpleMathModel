package experiment

import (
	"fmt"

	"math/rand"

	"math"

	"github.com/MaxHalford/gago"
	"github.com/apanichkina/KSAMSimpleMathModel/math/hive"
	"github.com/apanichkina/KSAMSimpleMathModel/math/test/partsupp_datavault"
	"github.com/apanichkina/KSAMSimpleMathModel/math/test2/anchor"
	"github.com/apanichkina/KSAMSimpleMathModel/math/test3/snowflake"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

type Globals []float64

func (g Globals) Set() {
	hive.Lw = g[0]
	hive.Lr = g[1]
	hive.Hw = g[2]
	hive.Hr = g[3]
	//hive.BlockSize = g[4]
	hive.Net = g[4]
}

func (g Globals) String() string {
	if len(g) != 5 {
		return "invalid globals"
	}
	return fmt.Sprintf(
		"Lw: %f, Lr: %f, Hw: %f, Hr: %f, Net: %f",
		g[0], g[1], g[2], g[3], g[4],
	)
}

func (g Globals) Evaluate() (float64, error) {
	g.Set()
	result := 0.0
	for _, e := range Experiments {
		result += e.Evaluate()
	}
	return result, nil
}

// Mutate a Vector by resampling each element from a normal distribution with
// probability 0.8.
func (g Globals) Mutate(rng *rand.Rand) {
	//for {
	for i, v := range g {
		if rand.Float64() > 0.2 {
			g[i] = rand.Float64()*2*v + rand.ExpFloat64()
		}
	}

	//	canBreak := true
	//	for _, v := range g {
	//		if v <= 1 {
	//			canBreak = false
	//		}
	//	}
	//	if canBreak {
	//		break
	//	}
	//}
}

// Crossover a Vector with another Vector by applying uniform crossover.
func (g Globals) Crossover(Y gago.Genome, rng *rand.Rand) {
	gago.CrossUniformFloat64(g, Y.(Globals), rng)
}

// Clone a Vector to produce a new one that points to a different slice.
func (g Globals) Clone() gago.Genome {
	var Y = make(Globals, len(g))
	copy(Y, g)
	return Y
}

// VectorFactory returns a random vector by generating 2 values uniformally
func VectorFactory(rng *rand.Rand) gago.Genome {
	return Globals(gago.InitUnifFloat64(5, 0, 10000, rng))
}

type ExperimentInterface interface {
	Evaluate() float64
}

type Experiment struct {
	parser.Query
	result float64
}

func (e Experiment) Evaluate() float64 {
	c := hive.EvaluateQueryPlan(e.Query)
	//fmt.Printf("[EXPECTED] got: %.2f, expected: %.2f\n", c.Seconds(), e.result)
	return math.Abs(c.Seconds() - e.result)
}

var Experiments []ExperimentInterface

func init() {
	Experiments = append(Experiments, GetApacheExperiments()...)
	Experiments = append(Experiments,
		experimentWithFunc{
			snowflake.GetSnowflake,
			220.23,
		},
		experimentWithFunc{
			anchor.GetAnchor,
			2597.23,
		},
		experimentWithFunc{
			partsupp_datavault.GetDataVault,
			92.94,
		},
	)
}

type experimentWithFunc struct {
	eval           func() hive.Cost
	expectedResult float64
}

func (e experimentWithFunc) Evaluate() float64 {
	c := e.eval()
	//fmt.Printf("[EXPECTED] got: %.2f, expected: %.2f\n", c.Seconds(), e.expectedResult)
	return math.Abs(c.Seconds()-e.expectedResult) * 1000
}
