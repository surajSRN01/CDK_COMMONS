from dataclasses import dataclass
import jsonpickle
from utilities import validator as validator

@dataclass
class Automobile:

    UNIQUE_BY = "_id"

    _id: str
    name: str
    comment: str 
    timeStamp: str 
    likes: str  
    replyCount: str 
    date: int
    time: str
    year: str 
    month: str 
    day: str  
    hour: str 
    session: int
    clean_text:int
    
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