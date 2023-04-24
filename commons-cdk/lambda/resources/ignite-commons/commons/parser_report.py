import datetime
from typing import List;

class ParserReport:      
    passed: int     
    failed: int  
    importDate: str
    logs:list
    feed_types: List[dict]
    total: int

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.importDate = datetime.datetime.now()
        self.logs = []
        self.feed_types = []
        self.total = 0

    def add_failed(self, log):
        self.failed += 1
        self.logs.append(log)
    
    def add_passes(self):        
        self.passed += 1