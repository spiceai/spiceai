# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/runtime/v1/runtime.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1eproto/runtime/v1/runtime.proto\x12\x07runtime\"2\n\x0b\x45xportModel\x12\x11\n\tdirectory\x18\x01 \x01(\t\x12\x10\n\x08\x66ilename\x18\x02 \x01(\t\"=\n\x0bImportModel\x12\x0b\n\x03pod\x18\x01 \x01(\t\x12\x0b\n\x03tag\x18\x02 \x01(\t\x12\x14\n\x0c\x61rchive_path\x18\x03 \x01(\t\"\xdb\x01\n\x07\x45pisode\x12\x0f\n\x07\x65pisode\x18\x01 \x01(\x03\x12\r\n\x05start\x18\x02 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x03 \x01(\x03\x12\r\n\x05score\x18\x04 \x01(\x01\x12\x39\n\ractions_taken\x18\x05 \x03(\x0b\x32\".runtime.Episode.ActionsTakenEntry\x12\r\n\x05\x65rror\x18\x06 \x01(\t\x12\x15\n\rerror_message\x18\x07 \x01(\t\x1a\x33\n\x11\x41\x63tionsTakenEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x04:\x02\x38\x01\"H\n\x06\x46light\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\x12\"\n\x08\x65pisodes\x18\x03 \x03(\x0b\x32\x10.runtime.Episode\"T\n\x03Pod\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x15\n\rmanifest_path\x18\x02 \x01(\t\x12\x14\n\x0cmeasurements\x18\x03 \x03(\t\x12\x12\n\ncategories\x18\x04 \x03(\t\"R\n\nTrainModel\x12\x1a\n\x12learning_algorithm\x18\x01 \x01(\t\x12\x17\n\x0fnumber_episodes\x18\x02 \x01(\x03\x12\x0f\n\x07loggers\x18\x03 \x03(\tB1Z/github.com/spiceai/spiceai/pkg/proto/runtime_pbb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.runtime.v1.runtime_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z/github.com/spiceai/spiceai/pkg/proto/runtime_pb'
  _EPISODE_ACTIONSTAKENENTRY._options = None
  _EPISODE_ACTIONSTAKENENTRY._serialized_options = b'8\001'
  _EXPORTMODEL._serialized_start=43
  _EXPORTMODEL._serialized_end=93
  _IMPORTMODEL._serialized_start=95
  _IMPORTMODEL._serialized_end=156
  _EPISODE._serialized_start=159
  _EPISODE._serialized_end=378
  _EPISODE_ACTIONSTAKENENTRY._serialized_start=327
  _EPISODE_ACTIONSTAKENENTRY._serialized_end=378
  _FLIGHT._serialized_start=380
  _FLIGHT._serialized_end=452
  _POD._serialized_start=454
  _POD._serialized_end=538
  _TRAINMODEL._serialized_start=540
  _TRAINMODEL._serialized_end=622
# @@protoc_insertion_point(module_scope)
