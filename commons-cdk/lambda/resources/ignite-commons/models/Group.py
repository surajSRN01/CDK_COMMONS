from dataclasses import dataclass
import jsonpickle
from typing import List
import models.Address as Address

@dataclass
class Group:

    UNIQUE_BY = "_id"
    
    _id: str
    groupId: str
    language: str
    country: str
    owner: str
    brands: List[str]
    urlId: str
    name: str
    address: Address.Address
    description: str
    dealers: None
    title: str

    def __init__(self):
        self.dealers = []
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