"""
File                :   update_configuration.py

Description         :   This will update the case for the equipment in lbt

Author              :   LivNSense Technologies

Date Created        :   18-07-2019

Date Last modified :    19-07-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    UPDATED_SUCCESSFULLY, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from Database.db_queries import MAKE_FALSE_POSTGRES_QUERY, \
    MAKE_TRUE_POSTGRES_QUERY
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation
from utilities.LoggerFile import log_error, log_debug
import traceback


class UpdateCase(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and respond for the performacne tag value
    """

    def __init__(self, console=None, equipment=None, case=None):
        """
        This will call the parent class to validate the connection
        :param console: console name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()
        self.console = console
        self.equipment = equipment
        self.case = case

    def update_case(self):
        """
        This will data on the bases of the eqipment and console name.This will give the overview for the
        dyanmic benchmaraking features
        :return: Json Response
        """

        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            self._psql_session.execute(MAKE_FALSE_POSTGRES_QUERY.format(self.console, self.equipment))

            self._psql_session.execute(MAKE_TRUE_POSTGRES_QUERY.format(self.console, self.equipment, self.case))

            return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY},
                                safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
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
def update_value(request, console=None, equipment=None, case=None):
    """
    This function will return the dynamic benchmarking overview
    :param console: Console name
    :param equipment: equipment name
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:

                obj = UpdateCase(console, equipment, case)
                return obj.update_case()
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
