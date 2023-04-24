from exception.error_code import ErrorCode

class RepositoryOperationException(Exception):
    
    message: str
    
    def __init__(self, message, *args: object) -> None:
        super().__init__(*args)
        self.message = message