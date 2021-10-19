class AiEngineException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
        self.type = "unknown_error"

    def get_error_body(self):
        return {"error": self.type, "error_message": self.message}

    def get_error_message(self):
        return f"{self.type} {self.message}"


class LawInvalidException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "invalid_law_expression"


class RewardInvalidException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "invalid_reward_function"


class DataSourceActionInvalidException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "invalid_datasource_action_expression"


class UnsupportedGymEnvironmentException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "unsupported_gym_environment"


class InvalidFieldsException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "invalid_fields"


class InvalidDataShapeException(AiEngineException):
    def __init__(self, message: str):
        super().__init__(message)
        self.type = "invalid_data_shape"
