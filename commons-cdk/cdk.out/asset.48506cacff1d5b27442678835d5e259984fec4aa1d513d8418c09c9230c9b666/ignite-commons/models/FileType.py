from dataclasses import dataclass
import jsonpickle

@dataclass
class FileType:

    name: str
    isOptional: bool
    encoding: str

    def __init__(self, name, isOptional) -> None:
        self.name = name
        self.isOptional = isOptional
        self.encoding = ""
        
    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        return setattr(self, item, value)

    