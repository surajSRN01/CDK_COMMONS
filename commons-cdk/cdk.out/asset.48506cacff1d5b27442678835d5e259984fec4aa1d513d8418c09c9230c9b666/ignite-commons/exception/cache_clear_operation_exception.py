from exception.error_code import ErrorCode

class CacheClearOpeationException(Exception):
    
    message: str

    def __init__(self, message, *args: object):
        super().__init__(*args)
        self.message = message 
