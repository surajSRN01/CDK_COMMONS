from dataclasses import dataclass
import jsonpickle
from utilities import validator as validator

@dataclass
class Car:

    UNIQUE_BY = "_id"

    _id: str
    web_scraper_order: str
    web_scraper_start_url: str 
    title: str 
    title_href: str  
    model: str 
    running: int
    fuel_type: str
    gear_type: str 
    registered_in: str 
    color: str  
    assembly: str 
    engine_capacity: int
    body_type:int
    
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