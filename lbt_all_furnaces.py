"""
File                :   lbt_furnaces_config.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pandas._libs import json

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, FEED_NAME, \
    EQUIPMENT, IS_ACTIVE, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import MULTIPLE_CONFIG_EQUIPMENT, \
    MULTIPLE_CONFIG_CASE_NAME_PERFORMACE_TAGS, NON_FURNACE_EXTERNAL_TARGETS, ALL_PERF_TAGS_FOR_NON_FURNACES
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation

from Database.db_connection import *

conn = pg_connection()


class lbt_all_furnaces(_PostGreSqlConnection):
    """
    This ApplicationInterface shows the single or multiple equipments of the external tags and performance tags values based on the timestamp
    """

    def __init__(self, query_params):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.query_params = query_params

    def get_furnaces(self):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            if self.query_params:
                equipment = self.query_params[EQUIPMENT].split(",")
                """
                This condition for used to it will select single equipment also even multiple equipment also
                based on user selection
                """
                if len(equipment) == 1:
                    equipment_param = '(' + str(equipment[0]) + ')'
                    equipment_param = '(' + str(equipment[0]) + ')'
                else:
                    equipment_param = tuple(equipment)
                perform_list_all = []
                try:
                    if self.query_params[IS_ACTIVE] == "true" and int(equipment[0]) < 15:
                        self._psql_session.execute(MULTIPLE_CONFIG_EQUIPMENT.format(self.query_params[IS_ACTIVE],
                                                                                    equipment_param,
                                                                                    self.query_params[FEED_NAME]))
                    elif int(equipment[0]) > 14:
                        self._psql_session.execute(NON_FURNACE_EXTERNAL_TARGETS.format(equipment_param))
                    else:
                        pass

                    df = pd.DataFrame(self._psql_session.fetchall())

                    dt = df.groupby('equipment_tag_name').apply(lambda x: x.to_json(orient='records'))

                    df.sort_values('parameter', ascending=True, inplace=True)
                    obj = {}
                    array = []
                    for each_data in dt:
                        for each in json.loads(each_data):
                            obj[each['equipment_tag_name']] = {
                                'external_targets': json.loads(each_data),
                                'performance_tags': None
                            }

                    perform = []
                    try:
                        self._psql_session.execute(
                            MULTIPLE_CONFIG_CASE_NAME_PERFORMACE_TAGS.format(equipment_param))

                    except Exception as e:
                        log_error('Exception due to get_furnaces Function: %s' + str(e))

                    performance_list = json.loads(json.dumps(self._psql_session.fetchall()))
                    perf_list = json.loads(json.dumps(performance_list))

                    try:
                        self._psql_session.execute(ALL_PERF_TAGS_FOR_NON_FURNACES.format(equipment_param))



                    except Exception as e:
                        log_error('Exception due to get_furnaces Function: %s' + str(e))
                    perameter_list = json.loads(json.dumps(self._psql_session.fetchall()))
                    perform_list = json.loads(json.dumps(perameter_list))

                    if len(perform_list) > 0:
                        for each_perform in perform_list:
                            perform_list_all.append(each_perform['result'])
                    else:
                        pass
                    for each_data in perf_list:
                        try:

                            obj[each_data["equipment_tag_name"]]["performance_tags"] = each_data['case_name']

                        except Exception as err:

                            pass
                    for each_data in perform_list_all:
                        try:
                            obj[each_data["equipment_tag_name"]]["performance_tags_list"] = each_data['parameter']

                        except Exception as err:
                            pass
                    return JsonResponse(obj, safe=False, status=200)

                except Exception as e:
                    log_error('Exception due to get_furnaces Function: %s' + str(e))
                    return JsonResponse({"message": str(e)}, safe=False)

        except AssertionError as e:
            log_error('Exception due to get_furnaces Function: %s' + str(e))
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
def get_lbt_furnace_equipment(request):
    """
    :param request: request django object
    :param IS_ACTIVE: IS_ACTIVE will be provided
    :param FEED_NAME: feed name will be provided
    :param equipment: equipment name will be provided
    :return: json response
    """
    obj = query_params = None
    try:

        query_params = {
            IS_ACTIVE: request.GET[IS_ACTIVE],
            FEED_NAME: request.GET[FEED_NAME],
            EQUIPMENT: request.GET[EQUIPMENT]
        }

    except:
        pass

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = lbt_all_furnaces(query_params)
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
