package hive

// QueryEvaluation describes plan evaluation, saves history of query execution and counts plan cost
type QueryEvaluationPlan struct {
	fullPlanCost Cost
}
