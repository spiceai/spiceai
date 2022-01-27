package aiengine

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/state"
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

	for _, s := range podState {
		ipcMutex.Lock()
		defer ipcMutex.Unlock()

		addDataRequest := getAddDataRequest(pod, s, ipcPath)

		if addDataRequest == nil {
			continue
		}

		zaplog.Sugar().Debug(aurora.BrightMagenta(fmt.Sprintf("Sending data of %d rows", (*s.Record()).NumRows())))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		record := pod.CachedRecord(false)

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

		s.Sent()
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
