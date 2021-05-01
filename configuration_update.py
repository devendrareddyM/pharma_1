"""
File                :   configuration_update.py

Description         :   This will update parameters for the selected algorithm

Author              :   LivNSense Technologies

Date Created        :   11-11-2019

Date Last modified  :   11-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import json
import time
import traceback

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from Database.Configuration import FLAG
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    PUT_REQUEST, UPDATED_SUCCESSFULLY, UTF8_FORMAT, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug, log_info
from Database.db_queries import UPDATE_PARAM_DATA
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _CassandraConnection, _TokenValidation
from Database.Configuration import NAME, TABLE_NAME


class UpdateAlgorithmParams(_CassandraConnection):
    """
    This class is responsible for updating the params for the algorithm
    """

    def __init__(self, request_payload, algorithm_name):
        """
        :param algorithm_name : this will have the algorithm name
        :param request_payload : request
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.algo_name = algorithm_name
        self.request_payload = request_payload

    """
    Updates the algorithm details
    """

    def update_algorithms(self):
        """
        This will return all the list of the algorithm in json format from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            LAST_MODIFIED_DATE = str(round(time.time() * 1000))
            for algo_data in self.request_payload:
                try:
                    query = UPDATE_PARAM_DATA.format(NAME,
                                                     TABLE_NAME,
                                                     algo_data["param_value"],
                                                     algo_data["unit"],
                                                     algo_data["description"],
                                                     algo_data["algo_tag"],
                                                     LAST_MODIFIED_DATE,
                                                     FLAG,
                                                     self.algo_name,
                                                     algo_data["file_param_name"])
                    self._csql_session.execute(query)
                    log_info(
                        "updated--------" + '---------Algorithm name-------' + str(
                            self.algo_name) + '----Param name-----' + str(
                            algo_data["file_param_name"]) + '------update param value-----' + str(
                            algo_data["param_value"]))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                    return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                        status=e.args[0][STATUS_KEY])

            return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY}, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def update_algorithm_params(request, algorithm_name):
    """
    This function will update the algorithm with the passed json values.
    :param request: request django object
    :param algorithm_name : this either can be none or else it will have the algorithm name
    :return: json response
    """
    obj = None
    try:
        if request.method == PUT_REQUEST:
            request_payload = json.loads(request.body.decode(UTF8_FORMAT))
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:

                obj = UpdateAlgorithmParams(request_payload, algorithm_name)
                return obj.update_algorithms()
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
