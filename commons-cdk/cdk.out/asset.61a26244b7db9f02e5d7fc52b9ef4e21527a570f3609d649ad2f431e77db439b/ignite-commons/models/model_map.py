import models.Person as Person
import models.Dependent as Dependent
import models.Occupation as Occupation
import models.Car as Car
import models.JpCar as JpCar
import models.Books as Books
import models.Automobile as Automobile



MODEL_MAP = {
    "com.ignite.model.person.Person" : Person.Person,
    "com.ignite.model.dependent.Dependent" : Dependent.Dependent,
    "com.ignite.model.occupation.Occupation" : Occupation.Occupation,
    "com.ignite.model.car.Car" : Car.Car,   
    "com.ignite.model.jpcar.JpCar" : JpCar.JpCar,
    "com.ignite.model.books.Books" : Books.Books,   
    "com.ignite.model.automobile.Automobile" : Automobile.Automobile    
}


def get_model_class_by_config(config):
    return MODEL_MAP.get(config)
        