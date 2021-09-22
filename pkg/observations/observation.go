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

func GetCsv(headers []string, observations []Observation, previewLines int) (string, string) {
	csv := strings.Builder{}
	var csvPreview string
	for i, o := range observations {
		csv.WriteString(strconv.FormatInt(o.Time, 10))
		for _, f := range headers {
			csv.WriteString(",")
			val, ok := o.Data[f]
			if ok {
				csv.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
			}
		}
		csv.WriteString("\n")
		if previewLines > 0 && (i+1 == previewLines || (previewLines >= i && i+1 == len(observations))) {
			csvPreview = csv.String()
		}
	}
	return csv.String(), csvPreview
}
