package database

import "testing"

func TestCompoundIndexAdviceRequiresConservativeEvidence(t *testing.T) {
	unmodified, err := CompileQuery(Filter{"a": int64(1), "b": int64(2)}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	one := 1
	limited, err := CompileQuery(Filter{"a": int64(1), "b": int64(2)}, QueryOptions{Limit: &one})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		query    QuerySpec
		examined int64
		retained uint64
		want     bool
	}{
		{name: "below document floor", query: unmodified, examined: 31, retained: 1},
		{name: "below amplification floor", query: unmodified, examined: 32, retained: 9},
		{name: "threshold", query: unmodified, examined: 32, retained: 8, want: true},
		{name: "no retained candidates", query: unmodified, examined: 32, want: true},
		{name: "query modifier suppresses advice", query: limited, examined: 128, retained: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			explain := finalizeExplainAdvice(test.query, ExplainResult{
				CompoundIndexOpportunity: true,
				IndexableConjunctPaths:   []string{"a", "b"},
				DocumentsExamined:        test.examined,
				CandidatesRetained:       test.retained,
			})
			if got := hasExplainAdvice(explain, "consider_compound_index"); got != test.want {
				t.Fatalf("advice=%+v got=%t want=%t", explain.Advice, got, test.want)
			}
		})
	}
}
