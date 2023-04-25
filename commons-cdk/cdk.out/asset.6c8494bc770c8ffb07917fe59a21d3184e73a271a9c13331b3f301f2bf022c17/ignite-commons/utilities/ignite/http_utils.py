import urllib3
from utilities import log_utils as log_utils


class HttpUtils():

    httpClient: None
     
    def __init__(self) -> None:        
        self.httpClient = urllib3.PoolManager()

    def get(self, endpoint: str, request_context, headers = None):
        # log_utils.print_info_logs('Getting response from endpoint: {}'.format(endpoint), request_context)
        response = None
        if headers == None:
            response = self.httpClient.request('GET', endpoint)
        else:
            response = self.httpClient.request('GET', endpoint,headers=headers)
        
        statusCode = self.getStatus(response)
        if statusCode == 200:
            content = self.getResponseContent(response)
            # log_utils.print_info_logs(str.format('Received response with status: {}', statusCode), request_context)
            return content

        log_utils.print_debug_logs(str.format("calling endpoint results in non success status: {}", 
                                              str(statusCode)), request_context)
        
        # return empty response
        return str({})
    
    def getStatus(self, response):
        return response.status_code if "status_code" in response else response.status
    
    def getResponseContent(self, response):
        return response.content if "content" in response else response.data