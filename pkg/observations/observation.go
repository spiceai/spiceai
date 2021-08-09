package observations

import (
	"strconv"
	"strings"
)

type Observation struct {
	Timestamp uint64
	Data      map[string]interface{}
}

func GetCsv(headers []string, observations []Observation, previewLines int) (string, string) {
	csv := strings.Builder{}
	var csvPreview string
	for i, o := range observations {
		csv.WriteString(strconv.FormatUint(o.Timestamp, 10))
		for _, f := range headers {
			csv.WriteString(",")
			dotIndex := strings.LastIndex(f, ".")
			if dotIndex > 0 {
				f = f[dotIndex+1:]
			}
			val, ok := o.Data[f]
			if ok && val != nil && val != "" {
				csv.WriteString(strconv.FormatFloat(val.(float64), 'f', -1, 64))
			}
		}
		csv.WriteString("\n")
		if previewLines > 0 && (i+1 == previewLines || previewLines == len(observations)) {
			csvPreview = csv.String()
		}
	}
	return csv.String(), csvPreview
}
