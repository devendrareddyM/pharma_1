"""
File                :   algorithms_list.py

Description         :   This will return all the algorithm names

Author              :   LivNSense Technologies

Date Created        :   11-11-2019

Date Last modified  :   11-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import json
import time
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    RECORDS, PARAMS, FILES, HTTP_AUTHORIZATION_TOKEN
from Database.db_queries import SELECT_PARAM_DATA, SELECT_FILE_DATA, SELECT_ALGORITHM_NAME
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED, \
    HTTP_403_FORBIDDEN
from Database.Authentiction_tokenization import _CassandraConnection, _TokenValidation
from Database.Configuration import NAME, TABLE_NAME


class AllAlgorithmList(_CassandraConnection):
    """
    This class is responsible for getting the data and respond for the algorithm list
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()

    """
    Gets the algorithm data by algorithm_name and type
    """

    def get_algorithms_by_name_and_type(self, algorithm_name):

        """
        This will return all the list of the algorithm in json format from the Database .
        :return: Json Responses
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            final_response = {PARAMS: self.get_param_data_by_algorithm_name(algorithm_name),
                              FILES: self.get_file_data_by_algorithm_name(algorithm_name)}

            return JsonResponse(final_response, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    """
    Method to get the parameter details by algorithm name
    """

    def get_param_data_by_algorithm_name(self, algorithm_name):
        parameters_data = {}
        file_query = SELECT_PARAM_DATA.format(NAME, TABLE_NAME, algorithm_name)
        result_set = self._csql_session.execute(file_query)
        df_data = pd.DataFrame(result_set)
        param_list = []

        if result_set is not None and df_data.shape[0]:
            for algorithm_name in df_data["algorithm_name"].unique():
                df_temp = df_data[df_data["algorithm_name"] == algorithm_name]
                parameters_data["configuration_files"] = df_temp.to_dict(orient=RECORDS)
        param_list.append(parameters_data)
        return param_list

    """
        Method to get the file details by algorithm name
    """

    def get_file_data_by_algorithm_name(self, algorithm_name):
        files_data = {}
        file_query = SELECT_FILE_DATA.format(NAME, TABLE_NAME, algorithm_name)
        result_set = self._csql_session.execute(file_query)
        df_data = pd.DataFrame(result_set)
        file_list = []

        if result_set is not None and df_data.shape[0]:
            for algorithm_name in df_data["algorithm_name"].unique():
                df_temp = df_data[df_data["algorithm_name"] == algorithm_name]
                files_data["configuration_files"] = df_temp.to_dict(orient=RECORDS)
        file_list.append(files_data)
        return file_list

    """
        Method to get all the algorithm list (DISTINCT DATA)
    """

    def get_algorithm_list(self):
        result_set = self._csql_session.execute(SELECT_ALGORITHM_NAME.format(NAME, TABLE_NAME))
        df_data = pd.DataFrame(result_set)
        algo_list = []
        if result_set is not None and df_data.shape[0]:
            algo_list.extend(df_data["algorithm_name"])
        dump = json.dumps(algo_list)
        return HttpResponse(dump, content_type='application/json')

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def get_algorithm_list(request, algorithm_name=None):
    """
    This function will return the algorithm list and will return error if generated.
    :param request: request django object
    :param algorithm_name : this either can be none or else it will have the algorithm name
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = AllAlgorithmList()
                if algorithm_name:
                    return obj.get_algorithms_by_name_and_type(algorithm_name)
                else:
                    return obj.get_algorithm_list()
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
