from utilities.singleton import Singleton

@Singleton
class FeedRuntimeContext():
    
    log_file_path: str
    has_consolidate_log_file: bool
    stackTrace: None

    def __init__(self) -> None:
        # set all the default properties here
        
        self.log_file_path = None
        self.has_consolidate_log_file = False
        self.stackTrace = None 

    
    