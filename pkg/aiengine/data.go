package aiengine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/proto/aiengine_pb"
	"github.com/spiceai/spice/pkg/state"
)

func SendData(pod *pods.Pod, podState ...*state.State) error {
	if len(podState) == 0 {
		// Nothing to do
		return nil
	}

	err := IsServerHealthy()
	if err != nil {
		return err
	}

	for _, s := range podState {
		if s == nil || !s.TimeSentToAIEngine.IsZero() {
			// Already sent
			continue
		}

		csv := strings.Builder{}
		csv.WriteString("time")
		for _, field := range s.Fields() {
			csv.WriteString(",")
			csv.WriteString(strings.ReplaceAll(field, ".", "_"))
		}
		csv.WriteString("\n")

		observationData := s.Observations()

		if len(observationData) == 0 {
			continue
		}

		csvChunk, csvPreview := observations.GetCsv(s.FieldNames(), observationData, 5)

		zaplog.Sugar().Debugf("Posting data to AI engine:\n%s", aurora.BrightYellow(fmt.Sprintf("%s%s...\n%d observations posted", csv.String(), csvPreview, len(observationData))))

		csv.WriteString(csvChunk)

		addDataRequest := &aiengine_pb.AddDataRequest{
			Pod:     pod.Name,
			CsvData: csv.String(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := aiengineClient.AddData(ctx, addDataRequest)
		if err != nil {
			return fmt.Errorf("failed to post new data to pod %s: %w", pod.Name, err)
		}

		if response.Error {
			return fmt.Errorf("failed to post new data to pod %s: %s", pod.Name, response.Result)
		}

		s.Sent()
	}

	return err
}
