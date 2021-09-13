package interpretations

import (
	"fmt"
	"time"

	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

type Interpretation struct {
	start   time.Time
	end     time.Time
	name    string
	actions []string
	tags    []string
}

func NewInterpretationFromProto(apiInterpretation *runtime_pb.Interpretation) (*Interpretation, error) {
	i, err := NewInterpretation(
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

func NewInterpretation(start time.Time, end time.Time, name string) (*Interpretation, error) {
	if end.Before(start) {
		return nil, fmt.Errorf("start must be same or before end")
	}

	return &Interpretation{
		start: start,
		end:   end,
		name:  name,
	}, nil
}

func (i *Interpretation) Start() time.Time {
	return i.start
}

func (i *Interpretation) End() time.Time {
	return i.end
}

func (i *Interpretation) Name() string {
	return i.name
}

func (i *Interpretation) Actions() []string {
	return i.actions
}

func (i *Interpretation) Tags() []string {
	return i.tags
}

func (i *Interpretation) AddActions(actions ...string) {
	i.actions = append(i.actions, actions...)
}

func (i *Interpretation) AddTags(tags ...string) {
	i.tags = append(i.tags, tags...)
}
