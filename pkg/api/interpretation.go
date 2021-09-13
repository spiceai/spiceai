package api

import (
	"github.com/spiceai/spiceai/pkg/interpretations"
)

type Interpretation struct {
	Start   int64
	End     int64
	Name    string
	Actions []string
	Tags    []string
}

func NewInterpretation(interpretation *interpretations.Interpretation) *Interpretation {
	return &Interpretation{
		Start:   interpretation.Start().Unix(),
		End:     interpretation.End().Unix(),
		Name:    interpretation.Name(),
		Actions: interpretation.Actions(),
		Tags:    interpretation.Tags(),
	}
}

func ApiInterpretations(interpretations []*interpretations.Interpretation) []*Interpretation{
	apiInterpretations := make([]*Interpretation, 0, len(interpretations))
	for _, i := range interpretations {
		apiInterpretations = append(apiInterpretations, i)
	}
}
