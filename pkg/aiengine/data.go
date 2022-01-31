package aiengine

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/state"
	spice_time "github.com/spiceai/spiceai/pkg/time"
)

var (
	ipcMutex sync.RWMutex
)

const ipcPath = "/tmp/spice_ipc.sock"

func SendData(pod *pods.Pod, podState ...*state.State) error {
	if len(podState) == 0 {
		// Nothing to do
		return nil
	}

	err := IsAIEngineHealthy()
	if err != nil {
		return err
	}

	for _, state := range podState {
		ipcMutex.Lock()
		defer ipcMutex.Unlock()

		addDataRequest := getAddDataRequest(pod, state, ipcPath)

		if addDataRequest == nil {
			continue
		}

		zaplog.Sugar().Debug(aurora.BrightMagenta(fmt.Sprintf("Sending data of %d rows", (*state.Record()).NumRows())))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// record := pod.CachedRecord(false)
		record := getProcessedRecord(pod, state)

		go func() {
			if _, err := os.Stat(ipcPath); os.IsExist(err) {
				os.Remove(ipcPath)
			}
			listener, err := net.Listen("unix", ipcPath)
			if err != nil {
				fmt.Printf("failed to create IPC server for pod %s: %s\n", pod.Name, err)
				return
			}
			defer listener.Close()
			unixListener := listener.(*net.UnixListener)
			unixListener.SetDeadline(time.Now().Add(time.Second * 2))

			connection, err := unixListener.Accept()
			if err != nil {
				fmt.Printf("failed to accept IPC connection for pod %s : %s\n", pod.Name, err)
				return
			}
			defer connection.Close()

			// writer := ipc.NewWriter(connection, ipc.WithSchema((*s.Record()).Schema()))
			writer := ipc.NewWriter(connection, ipc.WithSchema(record.Schema()))
			defer writer.Close()

			// writer.Write(*s.Record())
			writer.Write(record)
		}()

		response, err := aiengineClient.AddData(ctx, addDataRequest)
		if err != nil {
			return fmt.Errorf("failed to post new data to pod %s: %w", pod.Name, err)
		}

		if response.Error {
			return fmt.Errorf("failed to post new data to pod %s: %s", pod.Name, response.Message)
		}

		state.Sent()
	}

	return err
}

func getAddDataRequest(pod *pods.Pod, s *state.State, ipcPath string) *aiengine_pb.AddDataRequest {
	if s == nil || !s.TimeSentToAIEngine.IsZero() {
		// Already sent
		return nil
	}

	zaplog.Sugar().Debugf(
		"Posting data to AI engine:\n%s", aurora.BrightYellow(
			fmt.Sprintf("record of lenght %d posted", (*s.Record()).NumRows())))
	addDataRequest := &aiengine_pb.AddDataRequest{
		Pod:        pod.Name,
		UnixSocket: ipcPath,
	}

	return addDataRequest
}

func getProcessedRecord(pod *pods.Pod, state *state.State) arrow.Record {
	record := *state.Record()
	fields := []arrow.Field{record.Schema().Field(0)}

	pool := memory.NewGoAllocator()

	timeBuilderMap := make(map[string]*array.Int8Builder)
	for _, timeCategoryName := range pod.TimeCategoryNames() {
		for _, timeCategory := range pod.TimeCategories()[timeCategoryName] {
			fields = append(fields, arrow.Field{Name: timeCategory.FieldName, Type: arrow.PrimitiveTypes.Int8})
			timeBuilderMap[timeCategory.FieldName] = array.NewInt8Builder(pool)
		}
	}
	// fields = append(fields, record.Schema().Fields()[1:len(state.IdentifierNames())+len(state.MeasurementNames())+1]...)
	for _, field := range record.Schema().Fields()[1 : len(state.IdentifierNames())+len(state.MeasurementNames())+1] {
		fields = append(fields, arrow.Field{
			Name: strings.Join(append(strings.Split(state.Origin(), "."), strings.Split(field.Name, ".")[1:]...), "_"),
			Type: field.Type})
	}

	var categories []*dataspace.CategoryInfo
	categoryBuilderMap := make(map[string]*array.Int8Builder)
	var tags []string
	tagBuilderMap := make(map[string]*array.Int8Builder)
	for _, dataspace := range pod.Dataspaces() {
		if dataspace.Path() != state.Origin() {
			continue
		}
		for _, category := range dataspace.Categories() {
			categories = append(categories, category)
			for _, oneHotName := range category.EncodedFieldNames {
				fields = append(fields, arrow.Field{Name: oneHotName, Type: arrow.PrimitiveTypes.Int8})
				categoryBuilderMap[oneHotName] = array.NewInt8Builder(pool)
			}
		}
		for tagIndex, fqName := range dataspace.FqTags() {
			tagValue := dataspace.Tags()[tagIndex]
			tags = append(tags, tagValue)
			tagField := arrow.Field{Name: strings.ReplaceAll(fqName, ".", "_"), Type: arrow.PrimitiveTypes.Int8}
			fields = append(fields, tagField)
			tagBuilderMap[tagValue] = array.NewInt8Builder(pool)
		}
	}

	recordTimeValues := record.Column(0).(*array.Int64)
	tagCol := record.Column(int(record.NumCols() - 1)).(*array.List)
	tagOffsets := tagCol.Offsets()[1:]
	tagValues := tagCol.ListValues().(*array.String)
	tagPos := 0
	for rowIndex := 0; rowIndex < int(record.NumRows()); rowIndex++ {

		for _, timeCategoryName := range pod.TimeCategoryNames() {
			rowTime := time.Unix(recordTimeValues.Value(rowIndex), 0)
			var rowValue int
			switch timeCategoryName {
			case spice_time.CategoryMonth:
				rowValue = int(rowTime.Month())
			case spice_time.CategoryDayOfMonth:
				rowValue = rowTime.Day()
			case spice_time.CategoryDayOfWeek:
				rowValue = int(rowTime.Weekday())
			case spice_time.CategoryHour:
				rowValue = rowTime.Hour()
			}
			for _, timeCategory := range pod.TimeCategories()[timeCategoryName] {
				if timeCategory.Value == int(rowValue) {
					timeBuilderMap[timeCategory.FieldName].Append(1)
				} else {
					timeBuilderMap[timeCategory.FieldName].Append(0)
				}
			}
		}

		for _, category := range categories {
			recordValues := record.Column(state.ColumnMap()["cat."+category.Name]).(*array.String)
			for valueIndex, oneHotName := range category.EncodedFieldNames {
				if category.Values[valueIndex] == recordValues.Value(rowIndex) {
					categoryBuilderMap[oneHotName].Append(1)
				} else {
					categoryBuilderMap[oneHotName].Append(0)
				}
			}
		}
		if tagValues.IsValid(rowIndex) {
			for tagPos < int(tagOffsets[rowIndex]) {
				tagValue := tagValues.Value(tagPos)
				tagBuilderMap[tagValue].Append(1)
				tagPos++
			}
		}
		for _, builder := range tagBuilderMap {
			if builder.Len() <= rowIndex {
				builder.Append(0)
			}
		}
	}

	cols := []arrow.Array{record.Column(0)}
	for _, timeCategoryName := range pod.TimeCategoryNames() {
		for _, timeCategory := range pod.TimeCategories()[timeCategoryName] {
			builder := timeBuilderMap[timeCategory.FieldName]
			cols = append(cols, builder.NewArray())
			builder.Release()
		}
	}
	cols = append(cols, record.Columns()[1:len(state.IdentifierNames())+len(state.MeasurementNames())+1]...)
	for _, category := range categories {
		for _, oneHotName := range category.EncodedFieldNames {
			cols = append(cols, categoryBuilderMap[oneHotName].NewArray())
			categoryBuilderMap[oneHotName].Release()
		}
	}
	for _, tagValue := range tags {
		builder := tagBuilderMap[tagValue]
		cols = append(cols, builder.NewArray())
		builder.Release()
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, record.NumRows())
}
