from proto.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
FILL_FORWARD: FillType
FILL_ZERO: FillType

class AddDataRequest(_message.Message):
    __slots__ = ["pod", "unix_socket"]
    POD_FIELD_NUMBER: _ClassVar[int]
    UNIX_SOCKET_FIELD_NUMBER: _ClassVar[int]
    pod: str
    unix_socket: str
    def __init__(self, pod: _Optional[str] = ..., unix_socket: _Optional[str] = ...) -> None: ...

class AddInterpretationsRequest(_message.Message):
    __slots__ = ["indexed_interpretations", "pod"]
    INDEXED_INTERPRETATIONS_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    indexed_interpretations: _common_pb2.IndexedInterpretations
    pod: str
    def __init__(self, pod: _Optional[str] = ..., indexed_interpretations: _Optional[_Union[_common_pb2.IndexedInterpretations, _Mapping]] = ...) -> None: ...

class DataConnector(_message.Message):
    __slots__ = ["name", "params"]
    class ParamsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    name: str
    params: _containers.ScalarMap[str, str]
    def __init__(self, name: _Optional[str] = ..., params: _Optional[_Mapping[str, str]] = ...) -> None: ...

class DataSource(_message.Message):
    __slots__ = ["actions", "connector"]
    class ActionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    CONNECTOR_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.ScalarMap[str, str]
    connector: DataConnector
    def __init__(self, connector: _Optional[_Union[DataConnector, _Mapping]] = ..., actions: _Optional[_Mapping[str, str]] = ...) -> None: ...

class ExportModelRequest(_message.Message):
    __slots__ = ["pod", "tag"]
    POD_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    pod: str
    tag: str
    def __init__(self, pod: _Optional[str] = ..., tag: _Optional[str] = ...) -> None: ...

class ExportModelResult(_message.Message):
    __slots__ = ["model_path", "response"]
    MODEL_PATH_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    model_path: str
    response: Response
    def __init__(self, response: _Optional[_Union[Response, _Mapping]] = ..., model_path: _Optional[str] = ...) -> None: ...

class FieldData(_message.Message):
    __slots__ = ["fill_method", "initializer"]
    FILL_METHOD_FIELD_NUMBER: _ClassVar[int]
    INITIALIZER_FIELD_NUMBER: _ClassVar[int]
    fill_method: FillType
    initializer: float
    def __init__(self, initializer: _Optional[float] = ..., fill_method: _Optional[_Union[FillType, str]] = ...) -> None: ...

class HealthRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ImportModelRequest(_message.Message):
    __slots__ = ["import_path", "pod", "tag"]
    IMPORT_PATH_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    import_path: str
    pod: str
    tag: str
    def __init__(self, pod: _Optional[str] = ..., tag: _Optional[str] = ..., import_path: _Optional[str] = ...) -> None: ...

class InferenceRequest(_message.Message):
    __slots__ = ["inference_time", "pod", "tag"]
    INFERENCE_TIME_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    inference_time: int
    pod: str
    tag: str
    def __init__(self, pod: _Optional[str] = ..., tag: _Optional[str] = ..., inference_time: _Optional[int] = ...) -> None: ...

class InferenceResult(_message.Message):
    __slots__ = ["action", "confidence", "end", "response", "start", "tag"]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    action: str
    confidence: float
    end: int
    response: Response
    start: int
    tag: str
    def __init__(self, response: _Optional[_Union[Response, _Mapping]] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., action: _Optional[str] = ..., confidence: _Optional[float] = ..., tag: _Optional[str] = ...) -> None: ...

class InitRequest(_message.Message):
    __slots__ = ["actions", "actions_order", "datasources", "epoch_time", "external_reward_funcs", "fields", "granularity", "interpolation", "interval", "laws", "period", "pod"]
    class ActionsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class ActionsOrderEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class FieldsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: FieldData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FieldData, _Mapping]] = ...) -> None: ...
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_ORDER_FIELD_NUMBER: _ClassVar[int]
    DATASOURCES_FIELD_NUMBER: _ClassVar[int]
    EPOCH_TIME_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_REWARD_FUNCS_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    GRANULARITY_FIELD_NUMBER: _ClassVar[int]
    INTERPOLATION_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    LAWS_FIELD_NUMBER: _ClassVar[int]
    PERIOD_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.ScalarMap[str, str]
    actions_order: _containers.ScalarMap[str, int]
    datasources: _containers.RepeatedCompositeFieldContainer[DataSource]
    epoch_time: int
    external_reward_funcs: str
    fields: _containers.MessageMap[str, FieldData]
    granularity: int
    interpolation: bool
    interval: int
    laws: _containers.RepeatedScalarFieldContainer[str]
    period: int
    pod: str
    def __init__(self, pod: _Optional[str] = ..., period: _Optional[int] = ..., interval: _Optional[int] = ..., granularity: _Optional[int] = ..., epoch_time: _Optional[int] = ..., actions: _Optional[_Mapping[str, str]] = ..., actions_order: _Optional[_Mapping[str, int]] = ..., fields: _Optional[_Mapping[str, FieldData]] = ..., laws: _Optional[_Iterable[str]] = ..., datasources: _Optional[_Iterable[_Union[DataSource, _Mapping]]] = ..., external_reward_funcs: _Optional[str] = ..., interpolation: bool = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ["error", "message", "result"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    error: bool
    message: str
    result: str
    def __init__(self, result: _Optional[str] = ..., message: _Optional[str] = ..., error: bool = ...) -> None: ...

class StartTrainingRequest(_message.Message):
    __slots__ = ["epoch_time", "flight", "learning_algorithm", "number_episodes", "pod", "training_data_dir", "training_goal", "training_loggers"]
    EPOCH_TIME_FIELD_NUMBER: _ClassVar[int]
    FLIGHT_FIELD_NUMBER: _ClassVar[int]
    LEARNING_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    NUMBER_EPISODES_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    TRAINING_DATA_DIR_FIELD_NUMBER: _ClassVar[int]
    TRAINING_GOAL_FIELD_NUMBER: _ClassVar[int]
    TRAINING_LOGGERS_FIELD_NUMBER: _ClassVar[int]
    epoch_time: int
    flight: str
    learning_algorithm: str
    number_episodes: int
    pod: str
    training_data_dir: str
    training_goal: str
    training_loggers: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, pod: _Optional[str] = ..., number_episodes: _Optional[int] = ..., flight: _Optional[str] = ..., training_goal: _Optional[str] = ..., epoch_time: _Optional[int] = ..., learning_algorithm: _Optional[str] = ..., training_data_dir: _Optional[str] = ..., training_loggers: _Optional[_Iterable[str]] = ...) -> None: ...

class FillType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
