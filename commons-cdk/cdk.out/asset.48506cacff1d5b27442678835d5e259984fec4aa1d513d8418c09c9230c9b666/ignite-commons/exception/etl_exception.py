from exception.error_code import ErrorCode

class EtlException(Exception):
    
    message: str
    error_code: ErrorCode
    detail: str

    def __init__(self, message, error_code, *args: object) -> None:
        super().__init__(*args)
        self.message = message
        self.error_code = error_code