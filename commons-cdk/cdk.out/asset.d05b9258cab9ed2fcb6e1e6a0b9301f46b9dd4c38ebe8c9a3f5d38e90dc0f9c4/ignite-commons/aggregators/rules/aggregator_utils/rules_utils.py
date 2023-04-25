import calendar
import locale
from typing import List
from datetime import date, datetime, timedelta

from translate import Translator
from utilities import log_utils as log_utils

import pandas
from models import constants as constants
# from models.Dealer import Dealer
# from models.OpenInterval import OpenInterval
# from models.OpeningHours import OpeningHours
# from models.OpeningHoursLabel import OpeningHoursLabel
# from models.OpeningHoursStructure import OpeningHoursStructure
# from models.Service import Services
from utilities import iterator_utils as iterator_utils
from utilities import transformer as transformer
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext

# DAYS_IN_LANGUAGES = {}

# class RuleUtils:

#     SUNDAY: int = 0
#     SATURDAY:int = 6
#     NINE: str = "9"
#     CLOSED: str = "closed"
    
#     # def intersectOpeningHour(source: OpeningHoursStructure, destination: OpeningHoursStructure):
#     #     if source.startDate > destination.startDate:
#     #         destination.startDate = source.startDate
    
#     #     if source.endDate < destination.endDate:
#     #         destination.endDate = source.endDate

#     # def hasSpecialOpeningHours(service: Services):
        
    #     return getOrDefault(service, "openingHours") is not None and \
    #            getOrDefault(service.openingHours, "specialOpeningHours") is not None and \
    #            not iterator_utils.is_empty(service.openingHours.specialOpeningHours)
    
#     def addDaysToDate(target_date: date, days: int):
#         if target_date == None or days == 0:
#             return
        
#         return target_date + timedelta(days=days)
    
#     def getDealerIdLanguageMap(request_context, results):
#         dealerIdLanguageMap = {}

#         if len(results) > 0:
#             for dealer in results:
#                 if dealer.dealerId in dealerIdLanguageMap:
#                     languages = dealerIdLanguageMap[dealer.dealerId]
#                 else:
#                     languages = []
#                 languages.append(dealer.language.lower())
#                 dealerIdLanguageMap[dealer.dealerId] = languages

#         return dealerIdLanguageMap


#     # def mergeSpecialOpeningHours(specialOpeningHours: List[OpeningHoursStructure], source: OpeningHoursStructure):

#     #     if validator.is_nan(specialOpeningHours) or iterator_utils.is_empty(specialOpeningHours):
#     #         return

#     #     if validator.is_nan(source):
#     #         return

#     #     foundOverlap = False
#     #     for destination in specialOpeningHours:
#     #         destination: OpeningHoursStructure = specialOpeningHours

#     #         if (not transformer.is_object_has_valid_property(destination, "startDate") or
#     #             not validator.is_nan(destination.startDate)) and \
#     #             (not transformer.is_object_has_valid_property(destination, "endDate") or
#     #                 not validator.is_nan(destination.endDate)):
#     #             continue

#     #         previousDay: date = RuleUtils.addDaysToDate(destination.startDate, -1)
#     #         nextDay: date = RuleUtils.addDaysToDate(destination.endDate, 1)

#     #         if (not transformer.is_object_has_valid_property(source, "startDate") or
#     #             not validator.is_nan(source.startDate)) and \
#     #             (not transformer.is_object_has_valid_property(source, "endDate") or
#     #                 not validator.is_nan(source.endDate)):
#     #             continue

#     #         if RuleUtils.isBetweenDates(source.startDate, previousDay, nextDay) and \
#     #            RuleUtils.isBetweenDates(source.endDate, previousDay, nextDay):
#     #                 RuleUtils.mergeOpeningHour(source, destination)
#     #                 foundOverlap = True
#     #                 break

#     #     if not foundOverlap:
#     #         specialOpeningHours.append(source)


#     def isBetweenDates(source: date, startDate: date, endDate: date):
#         isBetweenDates = False
#         if not source < startDate and not source > endDate:
#             isBetweenDates = True
#         return isBetweenDates


#     def mergeOpeningHour(source: OpeningHoursStructure, destination: OpeningHoursStructure):
#         if source.startDate < destination.startDate:
#             destination.startDate = source.startDate

#         if source.endDate > destination.endDate:
#             destination.endDate = source.endDate

#     def closeDealerWhenMainServicesAreClosed(dealer: Dealer):
        
#         if getOrDefault(dealer, "services") == None:
#             return
         
#         services: List[Services]  = dealer.services
#         dealerOpeningHours = RuleUtils.computeDealerSpecialOpeningHours(services)
        
#         if dealerOpeningHours is not None and not \
#             getOrDefault(dealerOpeningHours, "specialOpeningHours") is not None and \
#             iterator_utils.is_empty(dealerOpeningHours.specialOpeningHours):

#                 dealersCurrentOpeningHours = dealer.openingHours
#                 if getOrDefault(dealersCurrentOpeningHours, "specialOpeningHours") is None or \
#                    iterator_utils.is_empty(dealersCurrentOpeningHours.specialOpeningHours):
#                         dealersCurrentOpeningHours = dealerOpeningHours

#     def computeDealerSpecialOpeningHours(services: List[Services]) -> OpeningHours:
#         dealerOpeningHours: OpeningHours = None
#         for service in services:
#             if not constants.nissan_service_type["SERVICE_CLOSED"] == service.name:
#                 if RuleUtils.hasSpecialOpeningHours(service):
#                     if dealerOpeningHours is None:
#                         dealerOpeningHours = service.openingHours
#                         dealerOpeningHours.regularOpeningHours = None
#                     else: 
#                         if getOrDefault(service, "openingHours") is not None: 
#                             RuleUtils.intersectSpecialOpeningHours(service.openingHours.specialOpeningHours,
#                                 dealerOpeningHours.specialOpeningHours)
#                 else:
                    
#                     return None
#         return dealerOpeningHours

#     def intersectSpecialOpeningHours(specialHours: List[OpeningHoursStructure],dealerSpecialHours:List[OpeningHoursStructure]):
#         for dealerSpecialHour in dealerSpecialHours[:]:
#             dealerSpecialHour: OpeningHoursStructure = dealerSpecialHour
#             foundOverlap = False
#             for structure in specialHours:
#                 structure: OpeningHoursStructure = structure
#                 if transformer.is_object_has_valid_property(structure, "startDate") and \
#                    transformer.is_object_has_valid_property(structure, "endDate") and \
#                    transformer.is_object_has_valid_property(dealerSpecialHour, "startDate") and \
#                    transformer.is_object_has_valid_property(dealerSpecialHour, "endDate"):
#                         # [A, B] intersects [X, Y] iff A <= Y && B <= X, where
#                         # A = structure.getStartDate()
#                         # B = structure.getEndDate()
#                         # X = dealerStructure.getStartDate()
#                         # Y = dealerStructure.getEndDate()
#                         if (compareDate(structure.startDate, dealerSpecialHour.endDate) <= 0 and
#                             compareDate(structure.endDate, dealerSpecialHour.startDate) >= 0):
#                             RuleUtils.intersectOpeningHour(structure, dealerSpecialHour)
#                             foundOverlap = True
#                             break

#             if not foundOverlap:
#                 dealerSpecialHours.remove(dealerSpecialHour)

# def convertOpeningHoursLabelToText(openingHours: OpeningHours, openingHoursLabel:List[OpeningHoursLabel], language: str):
#         openingHoursText = []

#         if openingHours is not None and openingHoursLabel is not None:

#             regularOpeningHours = getOrDefault(openingHours, "regularOpeningHours")        
#             if not iterator_utils.is_empty(regularOpeningHours):
#                 convertRegularOpeningHoursLabelToText(openingHoursText, regularOpeningHours, language, openingHoursLabel)
            
#             specialOpeningHours = getOrDefault(openingHours, "specialOpeningHours")
#             if not iterator_utils.is_empty(specialOpeningHours):
#                 convertSpecialOpeningHoursLabelToText(openingHoursText, specialOpeningHours, language, openingHoursLabel)
            
#         return openingHoursText    
# # def convertOpeningHoursToText(openingHours: OpeningHours, language: str):
# #         openingHoursText = []

# #         if openingHours is not None:
# #             hasRegularOpeningHours = False 
# #             regularOpeningHours = getOrDefault(openingHours, "regularOpeningHours")
# #             if regularOpeningHours is not None and not iterator_utils.is_empty(regularOpeningHours):
# #                 hasRegularOpeningHours = True 
# #                 convertRegularOpeningHoursToText(openingHoursText, regularOpeningHours, language)

# #             specialOpeningHours = getOrDefault(openingHours, "specialOpeningHours")        
# #             if hasRegularOpeningHours and specialOpeningHours is not None and not iterator_utils.is_empty(specialOpeningHours):
# #                 convertSpecialOpeningHoursToText(openingHoursText, specialOpeningHours)
# #             else:
# #                 openingHoursText.append("")
        
# #         return "".join(openingHoursText)  
    
# def compareDate(startDate, endDate):

#     if isinstance(startDate, datetime) and isinstance(endDate, datetime):
#         return endDate.timestamp() - startDate.timestamp()

#     dtstartDate = datetime.combine(startDate, datetime.min.time())
#     dtendDate = datetime.combine(endDate, datetime.min.time())
#     return dtstartDate - dtendDate

# def dayName(day: int, language: str):
    
#     day_names = list(calendar.day_name)

#     """
#     Java's calender API consider week days as :
#     [Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday]
    
#     Python's calender API, week stats with Monday
#     [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]
    
#     Convert Python calender week days array to start with Sunday
#     """
#     first = day_names[-1]
#     day_names.pop(-1)
#     day_names.insert(0, first)

#     day_name = None
    
#     if day >= RuleUtils.SUNDAY and day <= RuleUtils.SATURDAY:        
#         day_name_en = day_names[(day + 1) % 7]
#         try:
#             if language not in DAYS_IN_LANGUAGES.keys():
#                 DAYS_IN_LANGUAGES[language] = {}
    
#             if day_name_en not in DAYS_IN_LANGUAGES[language].keys():
#                 translator = Translator(to_lang=language, from_lang="en")   
#                 DAYS_IN_LANGUAGES[language][day_name_en] =  translator.translate(day_name_en).capitalize()

#             day_name = DAYS_IN_LANGUAGES[language][day_name_en]
#         except Exception as e:
#             request_context = FeedRuntimeContext.get_instance().request_context 
#             log_utils.print_info_logs("Week day name in locale:'{}' is not supported"+ 
#                                       ", fallback to default english".format(language), request_context)
#             day_name = day_name_en
#     return day_name

def getOrDefault(object, property):
    if transformer.is_object_has_valid_property(object, property):
        return object[property]
    return None

def getOrBlankString(object, property):
    if transformer.is_object_has_valid_property(object, property):
        return object[property]
    return ""

# def getOrElse(value, property, is_nan:bool or None, giveback:type or None):
#     """
#         value: can be an object or a json
#         property: is a field inside value
#         is_nan: 
#             if True: 
#                 function will apply (if validator.is_nan())
#             elif False:
#                 function will apply (if not validator.is_nan())
#             else (None):
#                 no validation applied

#         giveback:
#             if giveback == str: return ""
#             if giveback == int: return -1
#             if giveback == list: return []
#             if giveback == bool: return False

#             return None (if none of the above cases matches)
#     """
#     finalvalue = None
#     validator_check = False
#     try:
#         if hasattr(value, property): finalvalue = value[property]
#     except:
#         try: 
#             if property in value: finalvalue = value[property]
#         except:finalvalue = None

#     if is_nan == True: 
#         if validator.is_nan(finalvalue): validator_check = True
#     elif is_nan == False:
#         if not validator.is_nan(finalvalue): validator_check = True
#     else:
#         validator_check = True
    
#     if not validator_check: 
#         if giveback == str: return ""
#         if giveback == int: return 0
#         if giveback == list: return []
#         if giveback == bool: return False
#         return None
#     else:
#         if giveback == str: return finalvalue
#         if giveback == int: return 1
#         if giveback == list: return finalvalue
#         if giveback == bool: return True
#         return finalvalue

# # def convertRegularOpeningHoursToText(openingHoursText:list, regularOpeningHours: List[OpeningHoursStructure], language: str):
# #     regularOpeningHours = sorted(regularOpeningHours, key=lambda oh: oh["weekDay"])
   
# #     index = 0
# #     while index < len(regularOpeningHours):
# #         firstDayIndex = index
# #         currentWeekDay = int(regularOpeningHours[index].weekDay) - 1
# #         openingHoursText.append(dayName(currentWeekDay, language))
# #         sameOpenInterval = True

# #         while sameOpenInterval:
# #             todayOpeningHours = regularOpeningHours[index]
# #             if index + 1 == len(regularOpeningHours):
# #                 break

# #             tomorrowOpeningHours: OpeningHoursStructure = regularOpeningHours[index + 1]
# #             validSchedule: bool = todayOpeningHours.equalsClosed(tomorrowOpeningHours) or \
# #                 todayOpeningHours.equalsIntervals(tomorrowOpeningHours)
            
# #             if validSchedule and int(todayOpeningHours.weekDay) + 1 == int(tomorrowOpeningHours.weekDay):
# #                 index = index + 1
# #             else:
# #                 sameOpenInterval = False
    
# #         if firstDayIndex == index:
# #             openingHoursText.append(": ")
# #         else:
# #             openingHoursText.append("-")
# #             openingHoursText.append(dayName(index, language))
# #             openingHoursText.append(": ")
        
# #         openingHoursText.append(openingHoursTime(regularOpeningHours[index]))
# #         openingHoursText.append("\r\n")
# #         index = index + 1
        

# def openingHoursTime(openingHoursStructure: OpeningHoursStructure):
#     openingHoursTime: List = []
#     if openingHoursStructure is not None:
#         closed = getOrDefault(openingHoursStructure, "closed")
#         is_closed = transformer.get_boolean(closed) if closed is not None else False
#         if closed is not None and is_closed:
#             openingHoursTime.append("-")
#         else:
#             delimiter: str = ""
#             openIntervals: List[OpenInterval] = openingHoursStructure.openIntervals \
#                                               if getOrDefault(openingHoursStructure, "openIntervals") is not None else []
            
#             if not iterator_utils.is_empty(openIntervals):
#                 openIntervals = sorted(openIntervals, key=lambda openInterval: openInterval["openFrom"])

#                 for openInterval in openIntervals: 
#                     openInterval: OpenInterval = openInterval
#                     openingHoursTime.append(delimiter)
#                     openingHoursTime.append(openInterval.openFrom)
#                     openingHoursTime.append("-")
#                     openingHoursTime.append(openInterval.openUntil)
#                     delimiter = ", "
            
#     return "".join(openingHoursTime) 

# def openingHoursTimeWithLabels(openingHoursStructure: OpeningHoursStructure, openingHourLabels, language):
#     openingHoursTime: List = []
#     if openingHoursStructure is not None:
#         closed = getOrDefault(openingHoursStructure, "closed")
#         is_closed = transformer.get_boolean(closed) if closed is not None else False
#         if closed is not None and is_closed:
#             openingHoursTime.append(getDayLabel(8, openingHourLabels, language))
#         else:
#             delimiter: str = ""
#             openIntervals: List[OpenInterval] = openingHoursStructure.openIntervals \
#                                               if getOrDefault(openingHoursStructure, "openIntervals") is not None else []
            
#             if not iterator_utils.is_empty(openIntervals):
#                 openIntervals = sorted(openIntervals, key=lambda openInterval: openInterval["openFrom"])

#                 for openInterval in openIntervals: 
#                     openInterval: OpenInterval = openInterval
#                     openingHoursTime.append(delimiter)
#                     openingHoursTime.append(openInterval.openFrom)
#                     openingHoursTime.append("-")
#                     openingHoursTime.append(openInterval.openUntil)
#                     delimiter = ", "
            
#     return "".join(openingHoursTime) 


# def convertSpecialOpeningHoursToText(openingHoursText, specialOpeningHours: List[OpeningHoursStructure]):
#     specialOpeningHours = sorted(specialOpeningHours, key=lambda specialOpeningHour: specialOpeningHour["weekDay"])    

#     for specialDay in specialOpeningHours:
#         if specialDay is not None:
#             startDate = getOrDefault(specialDay, "startDate")
#             endDate = getOrDefault(specialDay, "endDate")
#             closed = getOrDefault(specialDay, "closed")
#             isClosed = transformer.get_boolean(closed) if closed is not None else False

#             if closed is not None and not isClosed:
#                 datePattern = "%d/%m/%Y"
#                 if startDate is not None and endDate is not None and startDate == endDate:
#                     openingHoursText.append(transformer.format_date(startDate, datePattern))
#                 else:
#                     openingHoursText.append(transformer.format_date(startDate, datePattern))
#                     openingHoursText.append("-")
#                     openingHoursText.append(transformer.format_date(endDate, datePattern))
#                 openingHoursText.append(": ")
#                 openingHoursText.append(openingHoursTime(specialDay))
#                 openingHoursText.append("\r\n")

# def convertOpeningHoursToText(openingHours: OpeningHours, language: str):
#     openingHoursText = []

#     if openingHours is not None:
#         hasRegularOpeningHours = False 
#         regularOpeningHours = getOrDefault(openingHours, "regularOpeningHours")
#         if regularOpeningHours is not None and not iterator_utils.is_empty(regularOpeningHours):
#             hasRegularOpeningHours = True 
#             convertRegularOpeningHoursToText(openingHoursText, regularOpeningHours, language)

#         specialOpeningHours = getOrDefault(openingHours, "specialOpeningHours")        
#         if hasRegularOpeningHours and specialOpeningHours is not None and not iterator_utils.is_empty(specialOpeningHours):
#             convertSpecialOpeningHoursToText(openingHoursText, specialOpeningHours)
#         else:
#             openingHoursText.append("")
    
#     if openingHoursText is not None and isinstance(openingHoursText, list):    
#         return "".join(openingHoursText)
    
#     return ""

# def convertSpecialOpeningHoursLabelToText(openingHoursText, specialOpeningHours, language, openingHoursLabels):
#     specialOpeningHours = sorted(specialOpeningHours, key=lambda specialOpeningHour: specialOpeningHour["weekDay"])    

#     for specialOpeningHour in specialOpeningHours:
#         specialOpeningHour: OpeningHoursStructure = specialOpeningHour 
#         startDate = getOrDefault(specialOpeningHour, "startDate")
#         endDate = getOrDefault(specialOpeningHour, "endDate")
#         closed = getOrDefault(specialOpeningHour, "closed")
#         isClosed = closed if closed is not None else False
#         if specialOpeningHour is not None and startDate is not None and endDate is not None:
#             pattern = "%d/%m/%Y"
#             if startDate == endDate:
#                 openingHoursText.append(transformer.format_date(startDate, pattern))
#             else:
#                 openingHoursText.append(transformer.format_date(startDate, pattern))
#                 openingHoursText.append("-")
#                 openingHoursText.append(transformer.format_date(startDate, pattern))
#             if closed is not None and closed:
#                 openingHoursText.append(": ")
#                 openingHoursText.append(getDayLabel(8, openingHoursLabels, language))
#                 openingHoursText.append("\r\n")
#             else:
#                 openingHoursText.append(": ")
#                 openingHoursText.append(openingHoursTime(specialOpeningHour))
#                 openingHoursText.append("\r\n")


# def convertOpeningHoursLabelToText(openingHours: OpeningHours, openingHoursLabel:List[OpeningHoursLabel], language: str):
#     openingHoursText = []

#     if openingHours is not None and openingHoursLabel is not None:

#         regularOpeningHours = getOrDefault(openingHours, "regularOpeningHours")        
#         if not iterator_utils.is_empty(regularOpeningHours):
#             convertRegularOpeningHoursLabelToText(openingHoursText, regularOpeningHours, language, openingHoursLabel)
        
#         specialOpeningHours = getOrDefault(openingHours, "specialOpeningHours")
#         if not iterator_utils.is_empty(specialOpeningHours):
#             convertSpecialOpeningHoursLabelToText(openingHoursText, specialOpeningHours, language, openingHoursLabel)
        
#     if openingHoursText is not None and isinstance(openingHoursText, list):    
#         return "".join(openingHoursText)
    
#     return ""


# def getDayLabel(day: int, openingHoursLabels: List[OpeningHoursLabel], language: str):
#     dayLabel = None
#     dayString = str(day+1)
#     if dayString == RuleUtils.NINE:
#         dayString = RuleUtils.CLOSED
    
#     for openingHoursLabel in openingHoursLabels:
#         openingHoursLabel: OpeningHoursLabel = openingHoursLabel
#         labelKey: str = getOrDefault(openingHoursLabel, "labelKey")
#         labelLanguage:str = getOrDefault(openingHoursLabel, "language")
#         if labelKey is not None and labelLanguage is not None:
#             if dayString == labelKey.lower() and language == labelLanguage.lower():
#                 dayLabel = getOrDefault(openingHoursLabel, "labelValue")
#                 break

#     if dayLabel is not None:
#         return dayLabel
#     elif dayLabel is None and dayString == RuleUtils.CLOSED:
#         return "-"
#     else:
#         return dayName(day, language)
     
# def convertRegularOpeningHoursLabelToText(openingHoursText: list,regularOpeningHours:List[OpeningHoursStructure], 
#                                          language: str,openingHoursLabel: List[OpeningHoursLabel]):

#     regularOpeningHours = sorted(regularOpeningHours, key=lambda regularOpeningHour: regularOpeningHour["weekDay"])    

#     index = 0
#     while index < len(regularOpeningHours):
#         firstDayIndex = index
#         currentWeekDay = int(regularOpeningHours[index].weekDay) - 1
#         label = getDayLabel(currentWeekDay, openingHoursLabel, language)
#         if label is not None:
#             openingHoursText.append(label)
        
#         sameOpenInterval = True
#         while sameOpenInterval:
#             todayOpeningHours: OpeningHoursStructure = regularOpeningHours[index]
#             if index + 1 == len(regularOpeningHours):
#                 break
#             tomorrowOpeningHours: OpeningHoursStructure = regularOpeningHours[index + 1]

#             validSchedule: bool = todayOpeningHours.equalsClosed(tomorrowOpeningHours) or \
#                                   todayOpeningHours.equalsIntervals(tomorrowOpeningHours)
            
#             if (validSchedule and int(todayOpeningHours.weekDay + 1) == int(tomorrowOpeningHours.weekDay)):
#                 index = index + 1
#             else:
#                 sameOpenInterval = False
#         if (firstDayIndex == index):
#             openingHoursText.append(": ")
#         else:
#             dayLable = getDayLabel(index, openingHoursLabel, language)
#             if dayLable is not None:
#                 openingHoursText.append("-")
#                 openingHoursText.append(dayLable)
#                 openingHoursText.append(": ")
        
#         value = openingHoursTimeWithLabels(regularOpeningHours[index], openingHoursLabel, language)
#         if value is not None:
#             openingHoursText.append(value)    
#             openingHoursText.append("\r\n")

#         index = index + 1


# def getDealerIdLanguageMap(request_context,parser_result):
#     try:
#         dealer_language_map = {} 
#         for dealer in parser_result["results"]:
#             lang_list=[]              
#             dealerId= dealer["dealerId"] if hasattr(dealer, "dealerId") else None
#             language=dealer["language"] if hasattr(dealer, "language") else None
#             if dealerId and language:                        
#                 if(dealerId in dealer_language_map):
#                     lang_list=dealer_language_map[dealerId]
#                     if language not in lang_list: lang_list.append(language)
#                 else:                                                   
#                     lang_list.append(language)
#                 dict_obj={dealerId:lang_list}
#                 dealer_language_map.update(dict_obj)
#         return dealer_language_map
    
#     except Exception as e :        
#         log_utils.print_error_logs(str(e), request_context)
#         raise e

# def date_to_dateTime(dateObject: date):
#     return datetime(year=dateObject.year, month=dateObject.month, day=dateObject.day)

# def compare_dates(date1: date, date2: date) -> int:
#     """
#     Compares two Dates for ordering.
    
#     @param date1
#     @param date2

#     date1 to be compared with date2.

#     @return  
#         0 if the both dates are equal\n
#         -1 if date1 is before date2\n
#         1 if date1 is after date2
#     """
#     if date1 == date2:
#         return 0
    
#     if date1 > date2:
#         return 0

#     if date1 < date2:
#         return -1    
