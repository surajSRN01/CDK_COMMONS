# from logging import Logger as logger

# import boto3
# import utilities.constant as const
# import utilities.prop_reader as prop_reader
# # from models.email_info import EmailInfo
# import utilities.log_utils as log_utils
# from utilities.ignite.feed_runtime_context import FeedRuntimeContext

# def send_email():
#     feed_runtime_context = FeedRuntimeContext.get_instance()
    
#     correlation_id = feed_runtime_context.x_correlation_id
#     env = feed_runtime_context.env

#     try:
#         log_utils.print_info_logs("preparing email")

#         AWS_REGION = prop_reader.get_prop(const.email_section, 'aws.region')
#         CHARSET = prop_reader.get_prop(const.email_section, 'charset')
#         client = boto3.client('ses',region_name=AWS_REGION)

#         response = client.send_email(
#             Destination ={
#                 'ToAddresses': emailInfo.recepients
#             },
#             Message={
#                 'Body': {
#                     'Html': {
#                         'Charset': CHARSET,
#                         'Data': emailInfo.body
#                     }
#                 },
#                 'Subject': {
#                     'Charset': CHARSET,
#                     'Data': emailInfo.subject
#                 }
#             },
#             Source = emailInfo.sender
#         )

#         log_utils.print_info_logs("Email sent! Message ID:{}".format(response['MessageId']))
#     except Exception as e:
#         log_utils.print_error_logs("Error while sending email, reason: {}".format(str(e)))
#         raise e