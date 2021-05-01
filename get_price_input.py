"""
File                :   inferential_logic.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import io
import traceback
import pandas as pd
import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation
from Database.db_queries import get_price_input_data
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from utilities.http_request import error_instance


class price_input_details(_PostGreSqlConnection):
    """
    This ApplicationInterface gives the all furnaces names along with id particularly for hot console 1
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        :param furnaces: request payload
        """
        super().__init__()

    def get_price_input__data(self, parameter=None):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            result = []
            try:
                self._psql_session.execute(get_price_input_data)
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    result_data = df['json_build_object'][0]
                    result.append(result_data)
            except Exception as e:
                log_error('Exception occurs due to: %s' + str(e))

            return JsonResponse(result, safe=False)
        except AssertionError as e:
            log_error('Exception in get_price_input api Function: %s' + str(e))
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
def get_gap_analysis_price_input_data(request):
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
                obj = price_input_details()
                return obj.get_price_input__data()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError as e:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
