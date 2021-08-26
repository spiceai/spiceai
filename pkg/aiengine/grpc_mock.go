package aiengine

import (
	"context"

	"github.com/spiceai/spice/pkg/proto/aiengine_pb"
	"google.golang.org/grpc"
)

type MockAIEngineClient struct {
	InitHandler          func(context.Context, *aiengine_pb.InitRequest, ...grpc.CallOption) (*aiengine_pb.Response, error)
	AddDataHandler       func(context.Context, *aiengine_pb.AddDataRequest, ...grpc.CallOption) (*aiengine_pb.Response, error)
	StartTrainingHandler func(context.Context, *aiengine_pb.StartTrainingRequest, ...grpc.CallOption) (*aiengine_pb.Response, error)
	GetInferenceHandler  func(context.Context, *aiengine_pb.InferenceRequest, ...grpc.CallOption) (*aiengine_pb.InferenceResult, error)
	GetHealthHandler     func(context.Context, *aiengine_pb.HealthRequest, ...grpc.CallOption) (*aiengine_pb.Response, error)
	ExportModelHandler   func(context.Context, *aiengine_pb.ExportModelRequest, ...grpc.CallOption) (*aiengine_pb.ExportModelResult, error)
	ImportModelHandler   func(context.Context, *aiengine_pb.ImportModelRequest, ...grpc.CallOption) (*aiengine_pb.Response, error)
	CloseHandler         func() error
}

func NewMockAIEngineClient(target string) (AIEngineClient, error) {
	return &MockAIEngineClient{}, nil
}

func (a *MockAIEngineClient) Init(ctx context.Context, in *aiengine_pb.InitRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	if a.InitHandler != nil {
		return a.InitHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) AddData(ctx context.Context, in *aiengine_pb.AddDataRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	if a.AddDataHandler != nil {
		return a.AddDataHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) StartTraining(ctx context.Context, in *aiengine_pb.StartTrainingRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	if a.StartTrainingHandler != nil {
		return a.StartTrainingHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) GetInference(ctx context.Context, in *aiengine_pb.InferenceRequest, opts ...grpc.CallOption) (*aiengine_pb.InferenceResult, error) {
	if a.GetInferenceHandler != nil {
		return a.GetInferenceHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) GetHealth(ctx context.Context, in *aiengine_pb.HealthRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	if a.GetHealthHandler != nil {
		return a.GetHealthHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) ExportModel(ctx context.Context, in *aiengine_pb.ExportModelRequest, opts ...grpc.CallOption) (*aiengine_pb.ExportModelResult, error) {
	if a.ExportModelHandler != nil {
		return a.ExportModelHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) ImportModel(ctx context.Context, in *aiengine_pb.ImportModelRequest, opts ...grpc.CallOption) (*aiengine_pb.Response, error) {
	if a.ImportModelHandler != nil {
		return a.ImportModelHandler(ctx, in, opts...)
	}

	return nil, nil
}

func (a *MockAIEngineClient) Close() error {
	if a.CloseHandler != nil {
		return a.CloseHandler()
	}

	return nil
}
