package observations

import (
	"strconv"
	"strings"
)

type Observation struct {
	Time int64
	Data map[string]float64
	Tags []string
}

func GetCsv(headers []string, observations []Observation) string {
	csv := strings.Builder{}
	for _, o := range observations {
		csv.WriteString(strconv.FormatInt(o.Time, 10))
		for _, f := range headers {
			csv.WriteString(",")
			val, ok := o.Data[f]
			if ok {
				csv.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			}
		}
		csv.WriteString("\n")
	}
	return csv.String()
}
