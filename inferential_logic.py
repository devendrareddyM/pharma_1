"""
File                :   inferential_logic.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, GREEN_TAG, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import FURNACES_NAME, INFERENTIAL_RESULT_FURNACE
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class inferential_logic(_PostGreSqlConnection):
    """
    This ApplicationInterface gives the all furnaces names along with id particularly for hot console 1
    """

    def __init__(self, equipment):
        """
        This will call the parent class to validate the connection and initialize the values
        :param furnaces: request payload
        """
        super().__init__()
        self.equipment = equipment


    def get_inferential(self):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            self.sync_time = None

            dict_data =[]
            try:
                self._psql_session.execute(INFERENTIAL_RESULT_FURNACE.format(self.equipment))

                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data = yaml.safe_load(
                    df.to_json(orient=RECORDS))

            except Exception as e:
                log_error('Exception due to get_inferential Function: %s' + str(e))
            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error('Exception due to get_inferential Function: %s' + str(e))
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
def get_inferential_logic_data(request, equipment= None):
    """
    :param request: request django object
    :return: json response
    """
    obj = None

    obj = query_params = None
    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = inferential_logic(equipment)
                return obj.get_inferential()
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
