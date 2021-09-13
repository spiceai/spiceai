package api

import (
	"time"

	"github.com/spiceai/spiceai/pkg/interpretations"
)

type Interpretation struct {
	Start   int64
	End     int64
	Name    string
	Actions []string
	Tags    []string
}

func NewInterpretationFromApi(apiInterpretation *Interpretation) (*interpretations.Interpretation, error) {
	i, err := interpretations.NewInterpretation(
		time.Unix(apiInterpretation.Start, 0),
		time.Unix(apiInterpretation.End, 0),
		apiInterpretation.Name,
	)

	if err != nil {
		return nil, err
	}

	i.AddActions(apiInterpretation.Actions...)
	i.AddTags(apiInterpretation.Tags...)

	return i, nil
}

func NewApiInterpretation(interpretation *interpretations.Interpretation) *Interpretation {
	return &Interpretation{
		Start:   interpretation.Start().Unix(),
		End:     interpretation.End().Unix(),
		Name:    interpretation.Name(),
		Actions: interpretation.Actions(),
		Tags:    interpretation.Tags(),
	}
}

func ApiInterpretations(interpretations []interpretations.Interpretation) []*Interpretation {
	apiInterpretations := make([]*Interpretation, 0, len(interpretations))
	for _, i := range interpretations {
		apiInterpretation := NewApiInterpretation(&i)
		apiInterpretations = append(apiInterpretations, apiInterpretation)
	}

	return apiInterpretations
}
