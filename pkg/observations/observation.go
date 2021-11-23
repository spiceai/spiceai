package observations

import (
	"sort"
	"strconv"
	"strings"
)

type Observation struct {
	Time         int64
	Identifiers  map[string]string
	Measurements map[string]float64
	Categories   map[string]string
	Tags         []string
}

func GetCsv(headers []string, tags []string, observations []Observation) string {
	csv := strings.Builder{}
	for _, o := range observations {
		csv.WriteString(strconv.FormatInt(o.Time, 10))
		for _, f := range headers {
			csv.WriteString(",")

			if identifier, ok := o.Identifiers[f]; ok {
				csv.WriteString(identifier)
				continue
			}

			if measurement, ok := o.Measurements[f]; ok {
				csv.WriteString(strconv.FormatFloat(measurement, 'f', -1, 64))
				continue
			}

			if category, ok := o.Categories[f]; ok {
				csv.WriteString(category)
				continue
			}
		}

		csv.WriteString(",")
		if len(o.Tags) > 0 {
			sort.Strings(o.Tags)
			csv.WriteString(strings.Join(o.Tags, " "))
		}

		csv.WriteString("\n")
	}
	return csv.String()
}
