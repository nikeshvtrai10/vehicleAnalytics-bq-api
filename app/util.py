"""
Adsinsight BQ API Util functions
Author: Aashna Shrestha, 2023
Organization: Beam Lab Pvt. Ltd.
"""

from __future__ import division, print_function, absolute_import

def verify_headers(headers):
    """
    Function to verify whether the required headers are present in the request
    """
    if not headers.get('clientId'):
        message = 'clientId header is missing'
        return True, 400, message
    if headers.get('clientId') != 'adsinsight':
        client_id = headers.get('clientId')
        message = 'clientId \''+client_id+'\' is forbidden access to the API endpoint'
        return True, 403, message
    return False, 200, ''
