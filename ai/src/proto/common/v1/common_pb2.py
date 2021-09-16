# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/common/v1/common.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='proto/common/v1/common.proto',
  package='common',
  syntax='proto3',
  serialized_options=b'Z.github.com/spiceai/spiceai/pkg/proto/common_pb',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x1cproto/common/v1/common.proto\x12\x06\x63ommon\"Y\n\x0eInterpretation\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x0f\n\x07\x61\x63tions\x18\x04 \x03(\t\x12\x0c\n\x04tags\x18\x05 \x03(\t\"-\n\x15InterpretationIndices\x12\x14\n\x08indicies\x18\x01 \x03(\rB\x02\x10\x01\"\xd0\x01\n\x16IndexedInterpretations\x12/\n\x0finterpretations\x18\x01 \x03(\x0b\x32\x16.common.Interpretation\x12\x38\n\x05index\x18\x02 \x03(\x0b\x32).common.IndexedInterpretations.IndexEntry\x1aK\n\nIndexEntry\x12\x0b\n\x03key\x18\x01 \x01(\x03\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.common.InterpretationIndices:\x02\x38\x01\x42\x30Z.github.com/spiceai/spiceai/pkg/proto/common_pbb\x06proto3'
)




_INTERPRETATION = _descriptor.Descriptor(
  name='Interpretation',
  full_name='common.Interpretation',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='start', full_name='common.Interpretation.start', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='end', full_name='common.Interpretation.end', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='name', full_name='common.Interpretation.name', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='actions', full_name='common.Interpretation.actions', index=3,
      number=4, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='tags', full_name='common.Interpretation.tags', index=4,
      number=5, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=40,
  serialized_end=129,
)


_INTERPRETATIONINDICES = _descriptor.Descriptor(
  name='InterpretationIndices',
  full_name='common.InterpretationIndices',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='indicies', full_name='common.InterpretationIndices.indicies', index=0,
      number=1, type=13, cpp_type=3, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=b'\020\001', file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=131,
  serialized_end=176,
)


_INDEXEDINTERPRETATIONS_INDEXENTRY = _descriptor.Descriptor(
  name='IndexEntry',
  full_name='common.IndexedInterpretations.IndexEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='common.IndexedInterpretations.IndexEntry.key', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='common.IndexedInterpretations.IndexEntry.value', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=312,
  serialized_end=387,
)

_INDEXEDINTERPRETATIONS = _descriptor.Descriptor(
  name='IndexedInterpretations',
  full_name='common.IndexedInterpretations',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='interpretations', full_name='common.IndexedInterpretations.interpretations', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='index', full_name='common.IndexedInterpretations.index', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_INDEXEDINTERPRETATIONS_INDEXENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=179,
  serialized_end=387,
)

_INDEXEDINTERPRETATIONS_INDEXENTRY.fields_by_name['value'].message_type = _INTERPRETATIONINDICES
_INDEXEDINTERPRETATIONS_INDEXENTRY.containing_type = _INDEXEDINTERPRETATIONS
_INDEXEDINTERPRETATIONS.fields_by_name['interpretations'].message_type = _INTERPRETATION
_INDEXEDINTERPRETATIONS.fields_by_name['index'].message_type = _INDEXEDINTERPRETATIONS_INDEXENTRY
DESCRIPTOR.message_types_by_name['Interpretation'] = _INTERPRETATION
DESCRIPTOR.message_types_by_name['InterpretationIndices'] = _INTERPRETATIONINDICES
DESCRIPTOR.message_types_by_name['IndexedInterpretations'] = _INDEXEDINTERPRETATIONS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Interpretation = _reflection.GeneratedProtocolMessageType('Interpretation', (_message.Message,), {
  'DESCRIPTOR' : _INTERPRETATION,
  '__module__' : 'proto.common.v1.common_pb2'
  # @@protoc_insertion_point(class_scope:common.Interpretation)
  })
_sym_db.RegisterMessage(Interpretation)

InterpretationIndices = _reflection.GeneratedProtocolMessageType('InterpretationIndices', (_message.Message,), {
  'DESCRIPTOR' : _INTERPRETATIONINDICES,
  '__module__' : 'proto.common.v1.common_pb2'
  # @@protoc_insertion_point(class_scope:common.InterpretationIndices)
  })
_sym_db.RegisterMessage(InterpretationIndices)

IndexedInterpretations = _reflection.GeneratedProtocolMessageType('IndexedInterpretations', (_message.Message,), {

  'IndexEntry' : _reflection.GeneratedProtocolMessageType('IndexEntry', (_message.Message,), {
    'DESCRIPTOR' : _INDEXEDINTERPRETATIONS_INDEXENTRY,
    '__module__' : 'proto.common.v1.common_pb2'
    # @@protoc_insertion_point(class_scope:common.IndexedInterpretations.IndexEntry)
    })
  ,
  'DESCRIPTOR' : _INDEXEDINTERPRETATIONS,
  '__module__' : 'proto.common.v1.common_pb2'
  # @@protoc_insertion_point(class_scope:common.IndexedInterpretations)
  })
_sym_db.RegisterMessage(IndexedInterpretations)
_sym_db.RegisterMessage(IndexedInterpretations.IndexEntry)


DESCRIPTOR._options = None
_INTERPRETATIONINDICES.fields_by_name['indicies']._options = None
_INDEXEDINTERPRETATIONS_INDEXENTRY._options = None
# @@protoc_insertion_point(module_scope)
