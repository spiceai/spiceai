# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/aiengine/v1/aiengine.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from proto.common.v1 import common_pb2 as proto_dot_common_dot_v1_dot_common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n proto/aiengine/v1/aiengine.proto\x12\x08\x61iengine\x1a\x1cproto/common/v1/common.proto\"\x81\x01\n\rDataConnector\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x33\n\x06params\x18\x02 \x03(\x0b\x32#.aiengine.DataConnector.ParamsEntry\x1a-\n\x0bParamsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x9c\x01\n\nDataSource\x12*\n\tconnector\x18\x01 \x01(\x0b\x32\x17.aiengine.DataConnector\x12\x32\n\x07\x61\x63tions\x18\x02 \x03(\x0b\x32!.aiengine.DataSource.ActionsEntry\x1a.\n\x0c\x41\x63tionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"I\n\tFieldData\x12\x13\n\x0binitializer\x18\x01 \x01(\x01\x12\'\n\x0b\x66ill_method\x18\x02 \x01(\x0e\x32\x12.aiengine.FillType\"\xa5\x04\n\x0bInitRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x0e\n\x06period\x18\x02 \x01(\x03\x12\x10\n\x08interval\x18\x03 \x01(\x03\x12\x13\n\x0bgranularity\x18\x04 \x01(\x03\x12\x12\n\nepoch_time\x18\x05 \x01(\x03\x12\x33\n\x07\x61\x63tions\x18\x06 \x03(\x0b\x32\".aiengine.InitRequest.ActionsEntry\x12>\n\ractions_order\x18\x07 \x03(\x0b\x32\'.aiengine.InitRequest.ActionsOrderEntry\x12\x31\n\x06\x66ields\x18\x08 \x03(\x0b\x32!.aiengine.InitRequest.FieldsEntry\x12\x0c\n\x04laws\x18\t \x03(\t\x12)\n\x0b\x64\x61tasources\x18\n \x03(\x0b\x32\x14.aiengine.DataSource\x12\x1d\n\x15\x65xternal_reward_funcs\x18\x0b \x01(\t\x12\x15\n\rinterpolation\x18\x0c \x01(\x08\x1a.\n\x0c\x41\x63tionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x1a\x33\n\x11\x41\x63tionsOrderEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x05:\x02\x38\x01\x1a\x42\n\x0b\x46ieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\"\n\x05value\x18\x02 \x01(\x0b\x32\x13.aiengine.FieldData:\x02\x38\x01\":\n\x08Response\x12\x0e\n\x06result\x18\x01 \x01(\t\x12\x0f\n\x07message\x18\x02 \x01(\t\x12\r\n\x05\x65rror\x18\x03 \x01(\x08\"M\n\x11\x45xportModelResult\x12$\n\x08response\x18\x01 \x01(\x0b\x32\x12.aiengine.Response\x12\x12\n\nmodel_path\x18\x02 \x01(\t\"\xc8\x01\n\x14StartTrainingRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x17\n\x0fnumber_episodes\x18\x02 \x01(\x03\x12\x0e\n\x06\x66light\x18\x03 \x01(\t\x12\x15\n\rtraining_goal\x18\x04 \x01(\t\x12\x12\n\nepoch_time\x18\x05 \x01(\x03\x12\x1a\n\x12learning_algorithm\x18\x06 \x01(\t\x12\x19\n\x11training_data_dir\x18\x07 \x01(\t\x12\x18\n\x10training_loggers\x18\x08 \x03(\t\"D\n\x10InferenceRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t\x12\x16\n\x0einference_time\x18\x03 \x01(\x03\"\x84\x01\n\x0fInferenceResult\x12$\n\x08response\x18\x01 \x01(\x0b\x32\x12.aiengine.Response\x12\r\n\x05start\x18\x02 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x03\x12\x0e\n\x06\x61\x63tion\x18\x04 \x01(\t\x12\x12\n\nconfidence\x18\x05 \x01(\x02\x12\x0b\n\x03tag\x18\x06 \x01(\t\"2\n\x0e\x41\x64\x64\x44\x61taRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x13\n\x0bunix_socket\x18\x02 \x01(\t\"i\n\x19\x41\x64\x64InterpretationsRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12?\n\x17indexed_interpretations\x18\x02 \x01(\x0b\x32\x1e.common.IndexedInterpretations\"\x0f\n\rHealthRequest\".\n\x12\x45xportModelRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t\"C\n\x12ImportModelRequest\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t\x12\x13\n\x0bimport_path\x18\x03 \x01(\t*+\n\x08\x46illType\x12\x10\n\x0c\x46ILL_FORWARD\x10\x00\x12\r\n\tFILL_ZERO\x10\x01\x32\x96\x04\n\x08\x41IEngine\x12\x31\n\x04Init\x12\x15.aiengine.InitRequest\x1a\x12.aiengine.Response\x12\x37\n\x07\x41\x64\x64\x44\x61ta\x12\x18.aiengine.AddDataRequest\x1a\x12.aiengine.Response\x12M\n\x12\x41\x64\x64Interpretations\x12#.aiengine.AddInterpretationsRequest\x1a\x12.aiengine.Response\x12\x43\n\rStartTraining\x12\x1e.aiengine.StartTrainingRequest\x1a\x12.aiengine.Response\x12\x45\n\x0cGetInference\x12\x1a.aiengine.InferenceRequest\x1a\x19.aiengine.InferenceResult\x12\x38\n\tGetHealth\x12\x17.aiengine.HealthRequest\x1a\x12.aiengine.Response\x12H\n\x0b\x45xportModel\x12\x1c.aiengine.ExportModelRequest\x1a\x1b.aiengine.ExportModelResult\x12?\n\x0bImportModel\x12\x1c.aiengine.ImportModelRequest\x1a\x12.aiengine.ResponseB2Z0github.com/spiceai/spiceai/pkg/proto/aiengine_pbb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.aiengine.v1.aiengine_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z0github.com/spiceai/spiceai/pkg/proto/aiengine_pb'
  _DATACONNECTOR_PARAMSENTRY._options = None
  _DATACONNECTOR_PARAMSENTRY._serialized_options = b'8\001'
  _DATASOURCE_ACTIONSENTRY._options = None
  _DATASOURCE_ACTIONSENTRY._serialized_options = b'8\001'
  _INITREQUEST_ACTIONSENTRY._options = None
  _INITREQUEST_ACTIONSENTRY._serialized_options = b'8\001'
  _INITREQUEST_ACTIONSORDERENTRY._options = None
  _INITREQUEST_ACTIONSORDERENTRY._serialized_options = b'8\001'
  _INITREQUEST_FIELDSENTRY._options = None
  _INITREQUEST_FIELDSENTRY._serialized_options = b'8\001'
  _FILLTYPE._serialized_start=1834
  _FILLTYPE._serialized_end=1877
  _DATACONNECTOR._serialized_start=77
  _DATACONNECTOR._serialized_end=206
  _DATACONNECTOR_PARAMSENTRY._serialized_start=161
  _DATACONNECTOR_PARAMSENTRY._serialized_end=206
  _DATASOURCE._serialized_start=209
  _DATASOURCE._serialized_end=365
  _DATASOURCE_ACTIONSENTRY._serialized_start=319
  _DATASOURCE_ACTIONSENTRY._serialized_end=365
  _FIELDDATA._serialized_start=367
  _FIELDDATA._serialized_end=440
  _INITREQUEST._serialized_start=443
  _INITREQUEST._serialized_end=992
  _INITREQUEST_ACTIONSENTRY._serialized_start=319
  _INITREQUEST_ACTIONSENTRY._serialized_end=365
  _INITREQUEST_ACTIONSORDERENTRY._serialized_start=873
  _INITREQUEST_ACTIONSORDERENTRY._serialized_end=924
  _INITREQUEST_FIELDSENTRY._serialized_start=926
  _INITREQUEST_FIELDSENTRY._serialized_end=992
  _RESPONSE._serialized_start=994
  _RESPONSE._serialized_end=1052
  _EXPORTMODELRESULT._serialized_start=1054
  _EXPORTMODELRESULT._serialized_end=1131
  _STARTTRAININGREQUEST._serialized_start=1134
  _STARTTRAININGREQUEST._serialized_end=1334
  _INFERENCEREQUEST._serialized_start=1336
  _INFERENCEREQUEST._serialized_end=1404
  _INFERENCERESULT._serialized_start=1407
  _INFERENCERESULT._serialized_end=1539
  _ADDDATAREQUEST._serialized_start=1541
  _ADDDATAREQUEST._serialized_end=1591
  _ADDINTERPRETATIONSREQUEST._serialized_start=1593
  _ADDINTERPRETATIONSREQUEST._serialized_end=1698
  _HEALTHREQUEST._serialized_start=1700
  _HEALTHREQUEST._serialized_end=1715
  _EXPORTMODELREQUEST._serialized_start=1717
  _EXPORTMODELREQUEST._serialized_end=1763
  _IMPORTMODELREQUEST._serialized_start=1765
  _IMPORTMODELREQUEST._serialized_end=1832
  _AIENGINE._serialized_start=1880
  _AIENGINE._serialized_end=2414
# @@protoc_insertion_point(module_scope)
