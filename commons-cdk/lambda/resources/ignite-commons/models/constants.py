
import pymongo

model_names = {
    "Person": "com.ignite.model.person.Person",
    "Occupation": "com.ignite.model.occupation.Occupation",
    "Dependent": "com.ignite.model.dependent.Dependent",
    "Car": "com.ignite.model.car.Car",
    "JpCar": "com.ignite.model.jpcar.JpCar",
    "Books": "com.ignite.model.books.Books",
    "Automobile": "com.ignite.model.automobile.Automobile"
}


separators = {
    "ID_SEPARATOR": "_",
    "ID_SUFFIX_SEPARATOR": "-"
}


collection_names_hint = {
    "com.ignite.model.person.Person": "person",
    "com.ignite.model.occupation.Occupation": "occupation",
    "com.ignite.model.dependent.Dependent": "dependent",
    "com.ignite.model.car.Car": "car",
    "com.ignite.model.jpcar.JpCar": "jpcar",
    "com.ignite.model.books.Books": "books",    
    "com.ignite.model.automobile.Automobile": "automobile"
}

class_merge_attributes_map = {
    "com.ignite.model.person.Person": [],
    "com.ignite.model.occupation.Occupation": [],
    "com.ignite.model.dependent.Dependent": [],
    "com.ignite.model.car.Car": [],
    "com.ignite.model.jpcar.JpCar": [],
    "com.ignite.model.books.Books": [],
    "com.ignite.model.automobile.Automobile": []
}
COLLECTIONS_INDEXES = {
    "person": [
        {
            "type": "SINGLE",
            "specifier": pymongo.ASCENDING,
            "index_name": "Id"
        }
    ],
    "jpcar": [
        {
            "type": "SINGLE",
            "specifier": pymongo.ASCENDING,
            "index_name": "Id"
        }
        ],
    "books": [
        {
            "type": "SINGLE",
            "specifier": pymongo.ASCENDING,
            "index_name": "Id"
        }
        ],
    "car": [
        {
            "type": "SINGLE",
            "specifier": pymongo.ASCENDING,
            "index_name": "Id"
        }
    ],
    "automobile": [
        {
            "type": "SINGLE",
            "specifier": pymongo.ASCENDING,
            "index_name": "Id"
        }
    ]
}
