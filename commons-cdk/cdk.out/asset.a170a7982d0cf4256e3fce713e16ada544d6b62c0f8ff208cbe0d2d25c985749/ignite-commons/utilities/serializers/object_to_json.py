from json import JSONEncoder
from models.Person import Person
from utilities import transformer as transformer
from utilities import validator as validator
from utilities.serializers import person_serializer

class IgniteJsonEncoder(JSONEncoder):
    
    def default(self, source):
        
        if isinstance(source, Person):
            person_serializer.serialize(source)

        if validator.is_nan(source):
            return 
            
        return source.__dict__