// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.12
// source: proto/aiengine/v1/aiengine.proto

package aiengine_pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	AIEngine_Init_FullMethodName               = "/aiengine.AIEngine/Init"
	AIEngine_AddData_FullMethodName            = "/aiengine.AIEngine/AddData"
	AIEngine_AddInterpretations_FullMethodName = "/aiengine.AIEngine/AddInterpretations"
	AIEngine_StartTraining_FullMethodName      = "/aiengine.AIEngine/StartTraining"
	AIEngine_GetInference_FullMethodName       = "/aiengine.AIEngine/GetInference"
	AIEngine_GetHealth_FullMethodName          = "/aiengine.AIEngine/GetHealth"
	AIEngine_ExportModel_FullMethodName        = "/aiengine.AIEngine/ExportModel"
	AIEngine_ImportModel_FullMethodName        = "/aiengine.AIEngine/ImportModel"
)

// AIEngineClient is the client API for AIEngine service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type AIEngineClient interface {
	Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (*Response, error)
	AddData(ctx context.Context, in *AddDataRequest, opts ...grpc.CallOption) (*Response, error)
	AddInterpretations(ctx context.Context, in *AddInterpretationsRequest, opts ...grpc.CallOption) (*Response, error)
	StartTraining(ctx context.Context, in *StartTrainingRequest, opts ...grpc.CallOption) (*Response, error)
	GetInference(ctx context.Context, in *InferenceRequest, opts ...grpc.CallOption) (*InferenceResult, error)
	GetHealth(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*Response, error)
	ExportModel(ctx context.Context, in *ExportModelRequest, opts ...grpc.CallOption) (*ExportModelResult, error)
	ImportModel(ctx context.Context, in *ImportModelRequest, opts ...grpc.CallOption) (*Response, error)
}

type aIEngineClient struct {
	cc grpc.ClientConnInterface
}

func NewAIEngineClient(cc grpc.ClientConnInterface) AIEngineClient {
	return &aIEngineClient{cc}
}

func (c *aIEngineClient) Init(ctx context.Context, in *InitRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_Init_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) AddData(ctx context.Context, in *AddDataRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_AddData_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) AddInterpretations(ctx context.Context, in *AddInterpretationsRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_AddInterpretations_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) StartTraining(ctx context.Context, in *StartTrainingRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_StartTraining_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) GetInference(ctx context.Context, in *InferenceRequest, opts ...grpc.CallOption) (*InferenceResult, error) {
	out := new(InferenceResult)
	err := c.cc.Invoke(ctx, AIEngine_GetInference_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) GetHealth(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_GetHealth_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) ExportModel(ctx context.Context, in *ExportModelRequest, opts ...grpc.CallOption) (*ExportModelResult, error) {
	out := new(ExportModelResult)
	err := c.cc.Invoke(ctx, AIEngine_ExportModel_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aIEngineClient) ImportModel(ctx context.Context, in *ImportModelRequest, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, AIEngine_ImportModel_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AIEngineServer is the server API for AIEngine service.
// All implementations should embed UnimplementedAIEngineServer
// for forward compatibility
type AIEngineServer interface {
	Init(context.Context, *InitRequest) (*Response, error)
	AddData(context.Context, *AddDataRequest) (*Response, error)
	AddInterpretations(context.Context, *AddInterpretationsRequest) (*Response, error)
	StartTraining(context.Context, *StartTrainingRequest) (*Response, error)
	GetInference(context.Context, *InferenceRequest) (*InferenceResult, error)
	GetHealth(context.Context, *HealthRequest) (*Response, error)
	ExportModel(context.Context, *ExportModelRequest) (*ExportModelResult, error)
	ImportModel(context.Context, *ImportModelRequest) (*Response, error)
}

// UnimplementedAIEngineServer should be embedded to have forward compatible implementations.
type UnimplementedAIEngineServer struct {
}

func (UnimplementedAIEngineServer) Init(context.Context, *InitRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Init not implemented")
}
func (UnimplementedAIEngineServer) AddData(context.Context, *AddDataRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddData not implemented")
}
func (UnimplementedAIEngineServer) AddInterpretations(context.Context, *AddInterpretationsRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddInterpretations not implemented")
}
func (UnimplementedAIEngineServer) StartTraining(context.Context, *StartTrainingRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartTraining not implemented")
}
func (UnimplementedAIEngineServer) GetInference(context.Context, *InferenceRequest) (*InferenceResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetInference not implemented")
}
func (UnimplementedAIEngineServer) GetHealth(context.Context, *HealthRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetHealth not implemented")
}
func (UnimplementedAIEngineServer) ExportModel(context.Context, *ExportModelRequest) (*ExportModelResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExportModel not implemented")
}
func (UnimplementedAIEngineServer) ImportModel(context.Context, *ImportModelRequest) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ImportModel not implemented")
}

// UnsafeAIEngineServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to AIEngineServer will
// result in compilation errors.
type UnsafeAIEngineServer interface {
	mustEmbedUnimplementedAIEngineServer()
}

func RegisterAIEngineServer(s grpc.ServiceRegistrar, srv AIEngineServer) {
	s.RegisterService(&AIEngine_ServiceDesc, srv)
}

func _AIEngine_Init_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).Init(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_Init_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).Init(ctx, req.(*InitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_AddData_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddDataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).AddData(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_AddData_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).AddData(ctx, req.(*AddDataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_AddInterpretations_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddInterpretationsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).AddInterpretations(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_AddInterpretations_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).AddInterpretations(ctx, req.(*AddInterpretationsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_StartTraining_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartTrainingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).StartTraining(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_StartTraining_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).StartTraining(ctx, req.(*StartTrainingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_GetInference_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InferenceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).GetInference(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_GetInference_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).GetInference(ctx, req.(*InferenceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_GetHealth_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HealthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).GetHealth(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_GetHealth_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).GetHealth(ctx, req.(*HealthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_ExportModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExportModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).ExportModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_ExportModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).ExportModel(ctx, req.(*ExportModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AIEngine_ImportModel_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ImportModelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AIEngineServer).ImportModel(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AIEngine_ImportModel_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AIEngineServer).ImportModel(ctx, req.(*ImportModelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// AIEngine_ServiceDesc is the grpc.ServiceDesc for AIEngine service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var AIEngine_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "aiengine.AIEngine",
	HandlerType: (*AIEngineServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Init",
			Handler:    _AIEngine_Init_Handler,
		},
		{
			MethodName: "AddData",
			Handler:    _AIEngine_AddData_Handler,
		},
		{
			MethodName: "AddInterpretations",
			Handler:    _AIEngine_AddInterpretations_Handler,
		},
		{
			MethodName: "StartTraining",
			Handler:    _AIEngine_StartTraining_Handler,
		},
		{
			MethodName: "GetInference",
			Handler:    _AIEngine_GetInference_Handler,
		},
		{
			MethodName: "GetHealth",
			Handler:    _AIEngine_GetHealth_Handler,
		},
		{
			MethodName: "ExportModel",
			Handler:    _AIEngine_ExportModel_Handler,
		},
		{
			MethodName: "ImportModel",
			Handler:    _AIEngine_ImportModel_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/aiengine/v1/aiengine.proto",
}
