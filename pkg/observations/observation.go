package observations

import (
	"strconv"
	"strings"
)

type Observation struct {
	Time       int64
	Data       map[string]float64
	Categories map[string]string
	Tags       []string
}

func GetCsv(headers []string, validTags []string, observations []Observation) string {
	csv := strings.Builder{}
	for _, o := range observations {
		csv.WriteString(strconv.FormatInt(o.Time, 10))
		for _, f := range headers {
			csv.WriteString(",")

			if f == "_tags" {
				var observationValidTags []string
				for _, observationTag := range o.Tags {
					for _, validTag := range validTags {
						if validTag == observationTag {
							observationValidTags = append(observationValidTags, observationTag)
						}
					}
				}
				csv.WriteString(strings.Join(observationValidTags, " "))
				continue
			}

			val, ok := o.Data[f]
			if ok {
				csv.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
				continue
			}

			category, ok := o.Categories[f]
			if ok {
				csv.WriteString(category)
				continue
			}
		}

		csv.WriteString("\n")
	}
	return csv.String()
}
