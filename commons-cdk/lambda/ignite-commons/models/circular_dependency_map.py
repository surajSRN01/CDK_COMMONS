from dataclasses import dataclass
from models import Automobile


"""
Place to maintain circular dependency
"""
@dataclass   
class CircularDependencyMap():
   
    this = None
    def __init__(self) -> None:
        
        from models import Group
        self.GROUP_DEALER = {"source": Group, "property": "dealers", "referenced": Automobile, "type": "list"}
        self.DEALER_GROUP = {"source": Automobile, "property": "group", "referenced": Group, "type": "object"}
        
    def get_referenced_class(class_name, property):
        referenced_class: None
        referenced_type: None
        instance = CircularDependencyMap.get_instance()
        for i in instance.__dict__:
            source = instance.__dict__[i]["source"]        
            if(instance.__dict__[i]["property"] == property and source == class_name):
                referenced_class = instance.__dict__[i]["referenced"]
                referenced_type = instance.__dict__[i]["type"]
                break

        return referenced_class, referenced_type

    def is_referenced_class(class_name, property):
        is_circular_referecned_class = False
        instance = CircularDependencyMap.get_instance()
        for i in instance.__dict__:
            source = instance.__dict__[i]["source"]        
            if(instance.__dict__[i]["property"] == property and source == class_name):
                is_circular_referecned_class = True
                break

        return is_circular_referecned_class

    def get_instance():
        if(CircularDependencyMap.this == None):
            CircularDependencyMap.this = CircularDependencyMap()
        return CircularDependencyMap.this

    def __getitem__(self, item):
        return getattr(self, item)   

    def __setitem__(self, item, value):
        return setattr(self, item, value)        