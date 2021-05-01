"""
File                :   lbt_feeds_config.py

Author              :   LivNSense Technologies

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""


import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, \
    TYPE_REQUEST, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import FEEDS_CONFIG_FURNACE_NAME
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class feeds_config(_PostGreSqlConnection):
    """
    This ApplicationInterface shows active Furnaces in the Database
    """

    def __init__(self, query_params):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.query_params = query_params

    def get_values(self):
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            temp = []

            if self.query_params:
                try:
                    self._psql_session.execute(FEEDS_CONFIG_FURNACE_NAME.format(self.query_params[TYPE_REQUEST]))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    temp = yaml.safe_load(
                        df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error('Exception due to get_values Function: %s' + str(e))
                return JsonResponse(temp, safe=False)

        except AssertionError as e:
            log_error('Exception due to get_values Function: %s' + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_feeds_config(request):
    """
    :param request: request django object
    :return: json response
    """
    obj = query_params = None
    try:

        query_params = {
            TYPE_REQUEST: request.GET[TYPE_REQUEST]
        }

    except:
        pass

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = feeds_config(query_params)
                return obj.get_values()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
