from datetime import datetime

import utilities.email_utils as email_utils
# from models.email_info import EmailInfo
from utilities import constant as constant
from utilities import log_utils as log_utils
from utilities import prop_reader as prop_reader
from utilities import validator as validator
from utilities import iterator_utils as iter_utils
from utilities.ignite.feed_runtime_context import FeedRuntimeContext

DATE_HOUR_FORMAT = "%Y-%d-%m %H:%M"

"""
Any preprocessing like gathering of emailInfo will be done here 
"""
def create_email_info_and_send_email():
    try:
        feedRuntimeContext = FeedRuntimeContext.get_instance()
        date_now = datetime.utcnow().strftime(DATE_HOUR_FORMAT)
        brand = feedRuntimeContext.brand
        country = feedRuntimeContext.country
        try:
            recipients = prop_reader.get_prop(constant.email_section, "ses.recepients.{brand}".format(brand=brand), feedRuntimeContext)
            recipients = list(filter(lambda recipient : not validator.is_nan(recipient),recipients.split(",")))
            if iter_utils.is_empty(recipients):
                raise Exception("Unable to send status email, recipient not found")
        except Exception as e:
                raise e 

        subject = None
        body = None

        status = feedRuntimeContext.etl_status

        sender = prop_reader.get_prop(constant.email_section, "ses.sender")
        current_feed_file_name = feedRuntimeContext.current_file_path.split("/")[-1]
        
        subject = prop_reader.get_prop(constant.email_section, "ses.subject.{}".format(status.value[0]))
        body = prop_reader.get_prop(constant.email_section, "ses.body.{}".format(status.value[0]))

        subject = subject.format(brand=brand, country=country, enviroment=feedRuntimeContext.env, feed_file_name=current_feed_file_name)
        body = body.format(dateProcessed=date_now, filePathLog=feedRuntimeContext.log_file_path)
        # emailInfo = EmailInfo(subject, body, sender, recipients)

        # email_info_log = " building email for '{recipients}' with subject '{subject}'" \
        #         .format(recipients=emailInfo.get_recepients_as_string(), subject=subject)
        
        # log_utils.print_info_logs(email_info_log)
        # email_utils.send_email(emailInfo)

    except Exception as e:
        raise e    
