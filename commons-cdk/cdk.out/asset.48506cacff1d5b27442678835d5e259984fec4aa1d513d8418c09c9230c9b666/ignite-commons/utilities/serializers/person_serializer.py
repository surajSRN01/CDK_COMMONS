# from models.Group import Group
# from models.OpeningHoursStructure import OpeningHoursStructure
# from utilities.dealers_v2.feed_runtime_context import FeedRuntimeContext
from aggregators.rules.aggregator_utils import rules_utils
# from models.Image import Image
# from models.News import News
from models.Person import Person
# from models.Service import Services
# from utilities import iterator_utils as iterator_utils
# from utilities import transformer as transformer
# from utilities import validator as validator

# # properties for OpeningHoursStructure
def serialize(person: Person):
    serialize_fullname(person)
    return person
    
    
#     serialize_address(dealer)
#     serialize_contact(dealer)
#     serialize_openingHours(dealer)
#     serialize_dealer_services(dealer)
#     serialize_remove_average_star_rating(dealer)
    
#     # ugly way to change json object market specific:
#     request_context = FeedRuntimeContext.get_instance().request_context
#     if request_context.country == "jp":
#         restructure_dealer_json_for_japan(dealer)
    
#     return dealer

# def serialize_picture(source, populate_in):
#     if rules_utils.getOrDefault(source, populate_in) is not None:
#         for entity in source[populate_in]:
#             if rules_utils.getOrDefault(entity,"picture") is None:
#                 continue
#             for key in Image.__annotations__.keys():
#                 if rules_utils.getOrDefault(entity.picture, key) is not None:
#                     continue
#                 if key != "url":
#                     entity.picture[key] = ""

def serialize_fullname(person: Person):
    
    if rules_utils.getOrDefault(person, "firstName") == None:
        if rules_utils.getOrDefault(person, "lastName") == None:
            return 
        return
    
    fullName = {}
    fullName["firstName"] = person.firstName
    fullName["lastName"] = person.lastName
    person.fullName =fullName["firstName"]+" "+fullName["lastName"]




# def serialize_address(dealer: Dealer):
#     if rules_utils.getOrDefault(dealer, "address") == None:
#         return
    
#     dealer.address["addressLine2"] = rules_utils.getOrBlankString(dealer.address, "addressLine2")
    
# def serialize_contact(dealer: Dealer):
#     if rules_utils.getOrDefault(dealer, "contact") == None:
#         return
    
#     # dealer.contact["email"] = rules_utils.getOrBlankString(dealer.contact, "email")
    
#     if rules_utils.getOrDefault(dealer.contact, "phones") == None:
#         dealer.phones = []
#         dealer.phones.append({})
        

# def serialize_openingHours(dealer: Dealer):

#     if rules_utils.getOrDefault(dealer, "openingHours") is None:
#         return
#     dealer.openingHours["openingHoursText"] = rules_utils.getOrBlankString(dealer.openingHours, "openingHoursText")
    
#     if (rules_utils.getOrDefault(dealer.openingHours,"specialOpeningHours") is not None):
#         for specialOpeningHour in dealer.openingHours["specialOpeningHours"]:
#             specialOpeningHour: OpeningHoursStructure = specialOpeningHour
            
#             # to respect API contract remove dealerId from dealer's OpeningHour
#             if hasattr(specialOpeningHour, "dealerId"):
#                 del specialOpeningHour.dealerId

#             if rules_utils.getOrDefault(specialOpeningHour,"startDate") is not None and \
#                rules_utils.getOrDefault(specialOpeningHour,"endDate") is not None:
#                 format = get_date_format()
#                 specialOpeningHour.startDate = transformer.format_date(specialOpeningHour.startDate, format) 
#                 specialOpeningHour.endDate = transformer.format_date(specialOpeningHour.endDate, format)
    
#     if (rules_utils.getOrDefault(dealer.openingHours,"regularOpeningHours") is not None):
#         for regularOpeningHour in dealer.openingHours["regularOpeningHours"]:
#             regularOpeningHour: OpeningHoursStructure = regularOpeningHour
            
#             # to respect API contract remove dealerId from dealer's OpeningHour
#             if hasattr(regularOpeningHour, "dealerId"):
#                 del regularOpeningHour.dealerId

#             if(rules_utils.getOrDefault(regularOpeningHour,"startDate") is not None and \
#                rules_utils.getOrDefault(regularOpeningHour,"endDate") is not None):
#                 format = get_date_format()
#                 regularOpeningHour.startDate = transformer.format_date(regularOpeningHour.startDate, format) 
#                 regularOpeningHour.endDate = transformer.format_date(regularOpeningHour.endDate, format)
    
# def serialize_dealer_services(dealer: Dealer):

#     if rules_utils.getOrDefault(dealer, "services") is None:
#         return
    
#     for service in dealer.services:
#         service: Services = service

#         if rules_utils.getOrDefault(service, "phones") == None:
#             service.phones = []
#             service.phones.append({})

#         if rules_utils.getOrDefault(service, "openingHours") is not None: 
#             if rules_utils.getOrDefault(service.openingHours,"specialOpeningHours") is not None:
#                 for specialOpeningHour in service.openingHours.specialOpeningHours:
#                     specialOpeningHour: OpeningHoursStructure = specialOpeningHour
#                     # to respect API contract remove dealerId from dealer's OpeningHour
#                     if hasattr(specialOpeningHour, "dealerId"):
#                         del specialOpeningHour.dealerId
#                     if hasattr(specialOpeningHour, "serviceId"):
#                         del specialOpeningHour.serviceId    

#             if rules_utils.getOrDefault(service.openingHours, "regularOpeningHours") is not None:
#                 for regularOpeningHour in service.openingHours.regularOpeningHours:
#                     regularOpeningHour: OpeningHoursStructure = regularOpeningHour
#                     # to respect API contract remove dealerId from dealer's OpeningHour
#                     if hasattr(regularOpeningHour, "dealerId"):
#                         del regularOpeningHour.dealerId
#                     if hasattr(regularOpeningHour, "serviceId"):
#                         del regularOpeningHour.serviceId       

# def restructure_dealer_json_for_japan(dealer: Dealer):
    
#     # add all the empty fields:
#     dealer["certifications"] = get_list_or_default(dealer, "certifications")
#     dealer["awards"] = get_list_or_default(dealer, "awards")
#     dealer["commitments"] = get_list_or_default(dealer, "commitments")
#     dealer["socialNetworks"] = get_list_or_default(dealer, "socialNetworks")
#     dealer["spokenLanguages"] = get_list_or_default(dealer, "spokenLanguages")
#     dealer["modules"] = get_list_or_default(dealer, "modules")
#     dealer["news"] = get_list_or_default(dealer, "news")
    
#     # 1. remove addressLine2 from address if not exist
#     if rules_utils.getOrDefault(dealer, "address") is not None:
#        if hasattr(dealer.address, "addressLine2") and validator.is_nan(dealer.address.addressLine2):
#             del dealer.address.addressLine2

#     # 2. remove email from contact if not exist
#     if rules_utils.getOrDefault(dealer, "contact") is not None:
#        if hasattr(dealer.contact, "email") and validator.is_nan(dealer.contact.email):
#             del dealer.contact.email

#     # 3. remove empty phone number from service
#     if rules_utils.getOrDefault(dealer, "services") is not None:
#        for service in dealer.services:
#             service: Services = service
#             has_phones = hasattr(service, "phones")
#             if has_phones and len(service.phones)== 1 and \
#                len(service.phones[0].keys()) == 0:
#                   del service.phones
        
#     return dealer
    

# def get_list_or_default(dealer: Dealer, listProperty: str):
#     values = rules_utils.getOrDefault(dealer, listProperty)
#     if values is None:
#         return []
    
#     if values is not None and iterator_utils.is_empty(values):
#         return []
    
#     return values

# def get_date_format():
#     format = "%Y-%m-%d %H:%M"
#     request_context = FeedRuntimeContext.get_instance().request_context
    
#     if request_context.country == "jp":
#         format = "%Y-%m-%d"
    
#     return format    
    
# def serialize_remove_average_star_rating(dealer: Dealer):
#         if rules_utils.getOrDefault(dealer, 'averageStarRating') is not None:
#             sales_flag = False
#             afterSales_flag = False
#             if rules_utils.getOrDefault(dealer.averageStarRating, 'sales') is not None and is_rating_empty(dealer.averageStarRating.sales):
#                 sales_flag = True
#                 delattr(dealer.averageStarRating, 'sales')
#             if rules_utils.getOrDefault(dealer.averageStarRating, 'afterSales') is not None and is_rating_empty(dealer.averageStarRating.afterSales):
#                 afterSales_flag = True
#                 delattr(dealer.averageStarRating, 'afterSales')
#             if sales_flag and afterSales_flag:
#                 delattr(dealer, 'averageStarRating')

# def is_rating_empty(rating):
#     if rules_utils.getOrDefault(rating, 'displayStars') is None and rules_utils.getOrDefault(rating, 'displayScore') is None and rules_utils.getOrDefault(rating, 'numberOfReviews') is None: 
#         return True
#     return False                