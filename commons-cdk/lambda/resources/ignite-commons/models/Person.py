from dataclasses import dataclass
import jsonpickle
from utilities import validator as validator

@dataclass
class Person:

    UNIQUE_BY = "_id"

    _id: str
    id: str
    firstName: str 
    lastName: str
    fullName: str 
    gender: str  
    country: str 
    age: int

    
    def __init__(self):
        pass
    
    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item, value):
        return setattr(self, item, value)
    
    def __hash__(self) -> int:
        return hash(jsonpickle.dumps(self.__dict__))
    
    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, self.__class__) and \
            self.__dict__ == __o.__dict__