package aiengine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/state"
)

func SendData(pod *pods.Pod, podState ...*state.State) error {
	if len(podState) == 0 {
		// Nothing to do
		return nil
	}

	err := IsAIEngineHealthy()
	if err != nil {
		return err
	}

	for _, s := range podState {
		addDataRequest := getAddDataRequest(pod, s)

		if addDataRequest == nil {
			continue
		}

		zaplog.Sugar().Debug(aurora.BrightMagenta(fmt.Sprintf("Sending data %d", len(addDataRequest.CsvData))))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		response, err := aiengineClient.AddData(ctx, addDataRequest)
		if err != nil {
			return fmt.Errorf("failed to post new data to pod %s: %w", pod.Name, err)
		}

		if response.Error {
			return fmt.Errorf("failed to post new data to pod %s: %s", pod.Name, response.Message)
		}

		s.Sent()
	}

	return err
}

func getAddDataRequest(pod *pods.Pod, s *state.State) *aiengine_pb.AddDataRequest {
	if s == nil || !s.TimeSentToAIEngine.IsZero() {
		// Already sent
		return nil
	}

	ds := pod.GetDataspace(s.Path())
	categories := ds.Categories()

	csv := strings.Builder{}
	csv.WriteString("time")
	for _, field := range s.FqMeasurementsNames() {
		csv.WriteString(",")
		csv.WriteString(strings.ReplaceAll(field, ".", "_"))
	}
	for _, category := range categories {
		for _, categoryFieldName := range category.EncodedFieldNames {
			csv.WriteString(",")
			csv.WriteString(categoryFieldName)
		}
	}
	for _, fqTagName := range ds.FqTags() {
		csv.WriteString(",")
		csv.WriteString(strings.ReplaceAll(fqTagName, ".", "_"))
	}
	csv.WriteString("\n")

	observationData := s.Observations()

	if len(observationData) == 0 {
		return nil
	}

	csvPreview := getData(&csv, pod.Epoch(), s.MeasurementsNames(), categories, ds.Tags(), observationData, 5)

	zaplog.Sugar().Debugf("Posting data to AI engine:\n%s", aurora.BrightYellow(fmt.Sprintf("%s%s...\n%d observations posted", csv.String(), csvPreview, len(observationData))))

	addDataRequest := &aiengine_pb.AddDataRequest{
		Pod:     pod.Name,
		CsvData: csv.String(),
	}

	return addDataRequest
}

func getData(csv *strings.Builder, epoch time.Time, fqMeasurementNames []string, categories []*dataspace.CategoryInfo, tags []string, observations []observations.Observation, previewLines int) string {
	epochTime := epoch.Unix()
	var csvPreview string
	for i, o := range observations {
		if o.Time < epochTime {
			continue
		}
		csv.WriteString(strconv.FormatInt(o.Time, 10))

		for _, f := range fqMeasurementNames {
			csv.WriteString(",")
			if measurement, ok := o.Measurements[f]; ok {
				csv.WriteString(strconv.FormatFloat(measurement, 'f', -1, 64))
			}
		}

		for _, category := range categories {
			for _, val := range category.Values {
				csv.WriteString(",")
				if foundVal, ok := o.Categories[category.Name]; ok && foundVal == val {
					csv.WriteString("1")
				} else {
					csv.WriteString("0")
				}
			}
		}

		for _, t := range tags {
			csv.WriteString(",")

			hasTag := false
			for _, observationTag := range o.Tags {
				if observationTag == t {
					hasTag = true
					break
				}
			}

			if hasTag {
				csv.WriteString("1")
			} else {
				csv.WriteString("0")
			}
		}
		csv.WriteString("\n")
		if previewLines > 0 && (i+1 == previewLines || (previewLines >= i && i+1 == len(observations))) {
			csvPreview = csv.String()
		}
	}
	return csvPreview
}
