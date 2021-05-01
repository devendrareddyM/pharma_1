"""
File                :   lbt_furnaces_config.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, FEED_NAME, IS_ACTIVE, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import FURNACES_CONFIG_EXTERNAL_TAGS, FURNACES_CONFIG_PERFORM_TAGS
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class furnaces_config(_PostGreSqlConnection):
    """
    This ApplicationInterface gives the external tags and performance tags case names based on selecting of furnaces name
    """

    def __init__(self, query_params):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.query_params = query_params

    def get_dict_data_values(self):

        dict_data = {
            "external_tags": [],
            "performace_tags": []
        }
        return dict_data


    def get_ext_and_perf_tags(self, dict_data):
        try:
            self._psql_session.execute(FURNACES_CONFIG_EXTERNAL_TAGS.format(self.query_params[IS_ACTIVE],
                                                                            self.query_params[FEED_NAME]))
            df = pd.DataFrame(self._psql_session.fetchall())

            dict_data["external_tags"] = dict_data["external_tags"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error('Exception due to get_ext_and_perf_tags Function: %s' + str(e))

        try:
            self._psql_session.execute(FURNACES_CONFIG_PERFORM_TAGS)
            df = pd.DataFrame(self._psql_session.fetchall())

            dict_data["performace_tags"] = dict_data["performace_tags"] + yaml.safe_load(
                df.to_json(orient=RECORDS))
        except Exception as e:
            log_error('Exception due to get_ext_and_perf_tags Function: %s' + str(e))




    def get_furnaces(self):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            dict_data = self.get_dict_data_values()

            self.get_ext_and_perf_tags(dict_data)

            return JsonResponse(dict_data, safe=False)


        except AssertionError as e:
            log_error('Exception due to : %s' + str(e))
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
def get_furnaces_config(request):
    """
    :param request: request django object
    :param IS_ACTIVE: IS_ACTIVE will be provided
    :param FEED_NAME: FEED_NAME name will be provided
    :return: json response
    """
    obj = query_params = None

    try:

        query_params = {
            IS_ACTIVE: request.GET[IS_ACTIVE],
            FEED_NAME: request.GET[FEED_NAME]
        }

    except:
        pass

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = furnaces_config(query_params)
                return obj.get_furnaces()
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
