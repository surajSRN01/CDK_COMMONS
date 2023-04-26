import calendar
import locale
from typing import List
from datetime import date, datetime, timedelta

from translate import Translator
from utilities import log_utils as log_utils

import pandas
from models import constants as constants
from utilities import iterator_utils as iterator_utils
from utilities import transformer as transformer
from utilities import validator as validator
from utilities.ignite.feed_runtime_context import FeedRuntimeContext

DAYS_IN_LANGUAGES = {}


def compareDate(startDate, endDate):

    if isinstance(startDate, datetime) and isinstance(endDate, datetime):
        return endDate.timestamp() - startDate.timestamp()

    dtstartDate = datetime.combine(startDate, datetime.min.time())
    dtendDate = datetime.combine(endDate, datetime.min.time())
    return dtstartDate - dtendDate

def dayName(day: int, language: str):
    
    day_names = list(calendar.day_name)

    """
    Java's calender API consider week days as :
    [Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday]
    
    Python's calender API, week stats with Monday
    [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday]
    
    Convert Python calender week days array to start with Sunday
    """
    first = day_names[-1]
    day_names.pop(-1)
    day_names.insert(0, first)

    day_name = None
    
    if day >= RuleUtils.SUNDAY and day <= RuleUtils.SATURDAY:        
        day_name_en = day_names[(day + 1) % 7]
        try:
            if language not in DAYS_IN_LANGUAGES.keys():
                DAYS_IN_LANGUAGES[language] = {}
    
            if day_name_en not in DAYS_IN_LANGUAGES[language].keys():
                translator = Translator(to_lang=language, from_lang="en")   
                DAYS_IN_LANGUAGES[language][day_name_en] =  translator.translate(day_name_en).capitalize()

            day_name = DAYS_IN_LANGUAGES[language][day_name_en]
        except Exception as e:
            request_context = FeedRuntimeContext.get_instance().request_context 
            log_utils.print_info_logs("Week day name in locale:'{}' is not supported"+ 
                                      ", fallback to default english".format(language), request_context)
            day_name = day_name_en
    return day_name

def getOrDefault(object, property):
    if transformer.is_object_has_valid_property(object, property):
        return object[property]
    return None

def getOrBlankString(object, property):
    if transformer.is_object_has_valid_property(object, property):
        return object[property]
    return ""