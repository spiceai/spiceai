# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from proto.aiengine.v1 import aiengine_pb2 as proto_dot_aiengine_dot_v1_dot_aiengine__pb2


class AIEngineStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Init = channel.unary_unary(
                '/aiengine.AIEngine/Init',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InitRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )
        self.AddData = channel.unary_unary(
                '/aiengine.AIEngine/AddData',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddDataRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )
        self.AddInterpretations = channel.unary_unary(
                '/aiengine.AIEngine/AddInterpretations',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddInterpretationsRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )
        self.StartTraining = channel.unary_unary(
                '/aiengine.AIEngine/StartTraining',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.StartTrainingRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )
        self.GetInference = channel.unary_unary(
                '/aiengine.AIEngine/GetInference',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceResult.FromString,
                )
        self.GetHealth = channel.unary_unary(
                '/aiengine.AIEngine/GetHealth',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.HealthRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )
        self.ExportModel = channel.unary_unary(
                '/aiengine.AIEngine/ExportModel',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelResult.FromString,
                )
        self.ImportModel = channel.unary_unary(
                '/aiengine.AIEngine/ImportModel',
                request_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ImportModelRequest.SerializeToString,
                response_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
                )


class AIEngineServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Init(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AddData(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AddInterpretations(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def StartTraining(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetInference(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetHealth(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ExportModel(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ImportModel(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_AIEngineServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Init': grpc.unary_unary_rpc_method_handler(
                    servicer.Init,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InitRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
            'AddData': grpc.unary_unary_rpc_method_handler(
                    servicer.AddData,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddDataRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
            'AddInterpretations': grpc.unary_unary_rpc_method_handler(
                    servicer.AddInterpretations,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddInterpretationsRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
            'StartTraining': grpc.unary_unary_rpc_method_handler(
                    servicer.StartTraining,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.StartTrainingRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
            'GetInference': grpc.unary_unary_rpc_method_handler(
                    servicer.GetInference,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceResult.SerializeToString,
            ),
            'GetHealth': grpc.unary_unary_rpc_method_handler(
                    servicer.GetHealth,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.HealthRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
            'ExportModel': grpc.unary_unary_rpc_method_handler(
                    servicer.ExportModel,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelResult.SerializeToString,
            ),
            'ImportModel': grpc.unary_unary_rpc_method_handler(
                    servicer.ImportModel,
                    request_deserializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ImportModelRequest.FromString,
                    response_serializer=proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'aiengine.AIEngine', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class AIEngine(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Init(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/Init',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InitRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AddData(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/AddData',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddDataRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AddInterpretations(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/AddInterpretations',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.AddInterpretationsRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def StartTraining(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/StartTraining',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.StartTrainingRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetInference(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/GetInference',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.InferenceResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetHealth(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/GetHealth',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.HealthRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ExportModel(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/ExportModel',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ExportModelResult.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ImportModel(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/aiengine.AIEngine/ImportModel',
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.ImportModelRequest.SerializeToString,
            proto_dot_aiengine_dot_v1_dot_aiengine__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
