from dataclasses import dataclass
from commons.parser_report import ParserReport

@dataclass
class ParserResult:      
    feedType: str
    results: list
    report: ParserReport
    clazz: str
    parserType: str 
    
    def __init__(self):
        self.feedType = ''
        self.results = []
        self.report = ParserReport()

    def add_result(self, result):
        self.results.append(result)       

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        return setattr(self, item, value) 
        