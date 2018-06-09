package main

import (
	"fmt"

	"os"

	"github.com/MaxHalford/gago"
	"github.com/apanichkina/KSAMSimpleMathModel/math/experiment/experiment"
)

func main() {
	var ga = gago.Generational(experiment.VectorFactory)

	var err = ga.Initialize()
	if err != nil {
		fmt.Println("Handle error!")
	}

	f, _ := os.Create("results.genomes")

	fmt.Printf("Best fitness at generation 0: %f\n", ga.HallOfFame[0].Fitness)
	for i := 1; i < 50000; i++ {
		if i%500 == 0 {
			f.WriteString(fmt.Sprintf("%s\n", ga.HallOfFame[0].Genome))
			fmt.Println(ga.HallOfFame[0].Genome)
		}
		err = ga.Evolve()
		if err != nil {
			fmt.Println("Handle error!")
		}
		fmt.Printf("Best fitness at generation %d: %f\n", i, ga.HallOfFame[0].Fitness)
	}
	fmt.Println("RESSSUUUUUULT")
	fmt.Println(ga.HallOfFame[0].Genome)
	ga.HallOfFame[0].Genome.Evaluate()
}
