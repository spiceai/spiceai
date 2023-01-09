from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Episode(_message.Message):
    __slots__ = ["actions_taken", "end", "episode", "error", "error_message", "score", "start"]
    class ActionsTakenEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    ACTIONS_TAKEN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    EPISODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    actions_taken: _containers.ScalarMap[str, int]
    end: int
    episode: int
    error: str
    error_message: str
    score: float
    start: int
    def __init__(self, episode: _Optional[int] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., score: _Optional[float] = ..., actions_taken: _Optional[_Mapping[str, int]] = ..., error: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class ExportModel(_message.Message):
    __slots__ = ["directory", "filename"]
    DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    directory: str
    filename: str
    def __init__(self, directory: _Optional[str] = ..., filename: _Optional[str] = ...) -> None: ...

class Flight(_message.Message):
    __slots__ = ["end", "episodes", "start"]
    END_FIELD_NUMBER: _ClassVar[int]
    EPISODES_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    end: int
    episodes: _containers.RepeatedCompositeFieldContainer[Episode]
    start: int
    def __init__(self, start: _Optional[int] = ..., end: _Optional[int] = ..., episodes: _Optional[_Iterable[_Union[Episode, _Mapping]]] = ...) -> None: ...

class ImportModel(_message.Message):
    __slots__ = ["archive_path", "pod", "tag"]
    ARCHIVE_PATH_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    archive_path: str
    pod: str
    tag: str
    def __init__(self, pod: _Optional[str] = ..., tag: _Optional[str] = ..., archive_path: _Optional[str] = ...) -> None: ...

class Pod(_message.Message):
    __slots__ = ["categories", "manifest_path", "measurements", "name"]
    CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_PATH_FIELD_NUMBER: _ClassVar[int]
    MEASUREMENTS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    categories: _containers.RepeatedScalarFieldContainer[str]
    manifest_path: str
    measurements: _containers.RepeatedScalarFieldContainer[str]
    name: str
    def __init__(self, name: _Optional[str] = ..., manifest_path: _Optional[str] = ..., measurements: _Optional[_Iterable[str]] = ..., categories: _Optional[_Iterable[str]] = ...) -> None: ...

class TrainModel(_message.Message):
    __slots__ = ["learning_algorithm", "loggers", "number_episodes"]
    LEARNING_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    LOGGERS_FIELD_NUMBER: _ClassVar[int]
    NUMBER_EPISODES_FIELD_NUMBER: _ClassVar[int]
    learning_algorithm: str
    loggers: _containers.RepeatedScalarFieldContainer[str]
    number_episodes: int
    def __init__(self, learning_algorithm: _Optional[str] = ..., number_episodes: _Optional[int] = ..., loggers: _Optional[_Iterable[str]] = ...) -> None: ...
