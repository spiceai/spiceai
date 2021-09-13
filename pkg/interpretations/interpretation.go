package interpretations

import (
	"fmt"
	"time"
)

type Interpretation struct {
	start   time.Time
	end     time.Time
	name    string
	actions []string
	tags    []string
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
