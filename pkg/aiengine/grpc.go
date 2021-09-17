package aiengine

import (
	"context"

	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"google.golang.org/grpc"
)

type AIEngineClient interface {
	aiengine_pb.AIEngineClient
	Close() error
}

type aiEngineClient struct {
	conn   *grpc.ClientConn
	client aiengine_pb.AIEngineClient
}

func NewAIEngineClient(target string) (AIEngineClient, error) {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := aiengine_pb.NewAIEngineClient(conn)

	return &aiEngineClient{
		conn:   conn,
		client: client,
	}, nil
}

func (a *aiEngineClient) Init(ctx context.Context, in *aiengine_pb.InitRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.Init(ctx, in, opts...)
}

func (a *aiEngineClient) AddData(ctx context.Context, in *aiengine_pb.AddDataRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.AddData(ctx, in, opts...)
}

func (a *aiEngineClient) AddInterpretations(ctx context.Context, in *aiengine_pb.AddInterpretationsRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.AddInterpretations(ctx, in, opts...)
}

func (a *aiEngineClient) StartTraining(ctx context.Context, in *aiengine_pb.StartTrainingRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.StartTraining(ctx, in, opts...)
}

func (a *aiEngineClient) GetInference(ctx context.Context, in *aiengine_pb.InferenceRequest, opts ...grpc.CallOption) (*aiengine_pb.InferenceResult, error) {
	return a.client.GetInference(ctx, in, opts...)
}

func (a *aiEngineClient) GetHealth(ctx context.Context, in *aiengine_pb.HealthRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.GetHealth(ctx, in, opts...)
}

func (a *aiEngineClient) ExportModel(ctx context.Context, in *aiengine_pb.ExportModelRequest, opts ...grpc.CallOption) (*aiengine_pb.ExportModelResult, error) {
	return a.client.ExportModel(ctx, in, opts...)
}

func (a *aiEngineClient) ImportModel(ctx context.Context, in *aiengine_pb.ImportModelRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	return a.client.ImportModel(ctx, in, opts...)
}

func (a *aiEngineClient) Close() error {
	err := a.conn.Close()
	if err != nil {
		return err
	}

	return nil
}
