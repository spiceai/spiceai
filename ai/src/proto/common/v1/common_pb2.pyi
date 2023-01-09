from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexedInterpretations(_message.Message):
    __slots__ = ["index", "interpretations"]
    class IndexEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: InterpretationIndices
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[InterpretationIndices, _Mapping]] = ...) -> None: ...
    INDEX_FIELD_NUMBER: _ClassVar[int]
    INTERPRETATIONS_FIELD_NUMBER: _ClassVar[int]
    index: _containers.MessageMap[int, InterpretationIndices]
    interpretations: _containers.RepeatedCompositeFieldContainer[Interpretation]
    def __init__(self, interpretations: _Optional[_Iterable[_Union[Interpretation, _Mapping]]] = ..., index: _Optional[_Mapping[int, InterpretationIndices]] = ...) -> None: ...

class Interpretation(_message.Message):
    __slots__ = ["actions", "end", "name", "start", "tags"]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.RepeatedScalarFieldContainer[str]
    end: int
    name: str
    start: int
    tags: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., name: _Optional[str] = ..., actions: _Optional[_Iterable[str]] = ..., tags: _Optional[_Iterable[str]] = ...) -> None: ...

class InterpretationIndices(_message.Message):
    __slots__ = ["indicies"]
    INDICIES_FIELD_NUMBER: _ClassVar[int]
    indicies: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, indicies: _Optional[_Iterable[int]] = ...) -> None: ...

class Observation(_message.Message):
    __slots__ = ["categories", "identifiers", "measurements", "tags", "time"]
    class CategoriesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class IdentifiersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class MeasurementsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIERS_FIELD_NUMBER: _ClassVar[int]
    MEASUREMENTS_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_NUMBER: _ClassVar[int]
    categories: _containers.ScalarMap[str, str]
    identifiers: _containers.ScalarMap[str, str]
    measurements: _containers.ScalarMap[str, float]
    tags: _containers.RepeatedScalarFieldContainer[str]
    time: int
    def __init__(self, time: _Optional[int] = ..., identifiers: _Optional[_Mapping[str, str]] = ..., measurements: _Optional[_Mapping[str, float]] = ..., categories: _Optional[_Mapping[str, str]] = ..., tags: _Optional[_Iterable[str]] = ...) -> None: ...
