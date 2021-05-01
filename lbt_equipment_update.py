"""
File                :   lbt_equipment_update.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pandas._libs import json
from psycopg2.extras import execute_values

from Database.db_connection import pg_connection

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, EXCEPTION_CAUSE, METHOD_NOT_ALLOWED, FEED_NAME, \
    EQUIPMENT, IS_ACTIVE, PUT_REQUEST, CASE_NAME, SET_LIMIT, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import SET_UPDATE_EXTERNAL_TAGS_LBT, SET_UPDATE_PERFORM_TAGS_LBT, \
    UPDATE_CASE_EQUIPMENT_MAPPING, INSERT_CASE_EQUIP_MAPPING, UPDATED_QUERY_FOR_EXTERANL_TARGETS_NON_FURNACE

from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation

"""
This method updating case name for performance Tags
"""


def update_perf(json_data, EQUIPMENT, IS_ACTIVE):
    update_perf = SET_UPDATE_PERFORM_TAGS_LBT.format(str(json_data),EQUIPMENT)
    return update_perf


class lbt_equipment_update(_PostGreSqlConnection):
    """
    This ApplicationInterface is used for updating the single or multiple equipments for external tags and
    performance tags based on user changes
    """

    def __init__(self, query_params, req_body):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        :param req_body:  req_body will be provided
        """
        super().__init__()
        self.query_params = query_params
        self.req_body = req_body

    def update_equip(self, body, equipment, feed_name, is_active):
        """
        This function will update external targets
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            conn = pg_connection()
            if conn:
                cursor = conn.cursor()
                try_now = list(body.keys())
                count = 0
                counter = 0
                performance_case_name = body[try_now[count]]['performance_tags']

                if is_active == 'true' and int(equipment[0]) < 15:
                    try:
                        try:
                            for each_equipment in equipment:
                                try:
                                    for each in json.loads(json.dumps(body[try_now[counter]]['external_targets'])):
                                        if float(each["min"]) <= float(each['target']):
                                            update_external_lbt = SET_UPDATE_EXTERNAL_TAGS_LBT.format(
                                                ('Between' + ' ' + each["min"] + ' and ' + each["max"]),
                                                each['target'],
                                                each['is_active'],
                                                is_active,
                                                each_equipment,
                                                feed_name,
                                                each['parameter'])
                                            cursor.execute(update_external_lbt)
                                        else:
                                            pass
                                    counter+=1
                                except Exception as e:
                                    pass
                            try:
                                conn.commit()
                            except Exception as commit_err:
                                log_error(commit_err)

                        except Exception as e:
                            log_error("The Exception is"+str(e))
                        if int(equipment[0]) < 15:
                            for each_equipment in equipment:
                                update_perf_lbt = update_perf(body[try_now[count]]['performance_tags'], each_equipment,
                                                              is_active)
                                cursor.execute(update_perf_lbt)
                                count += 1
                        else:
                            pass
                    except Exception as err:
                        pass
                elif int(equipment[0]) > 14:
                    try:
                        for each in json.loads(json.dumps(body[try_now[count]]['external_targets'])):
                            if float(each["min"]) <= float(each['target']):
                                update_external_lbt = UPDATED_QUERY_FOR_EXTERANL_TARGETS_NON_FURNACE.format(
                                    ('Between' + ' ' + each["min"] + ' and ' + each["max"]),
                                    each['target'],
                                    each['is_active'],
                                    equipment[0],
                                    each['parameter'])

                                cursor.execute(update_external_lbt)
                            else:
                                return JsonResponse({MESSAGE_KEY: SET_LIMIT},status=404)

                        if int(equipment[0]) > 14:
                            update_perf_lbt = update_perf(body[try_now[0]]['performance_tags'], equipment[0], is_active)
                            cursor.execute(update_perf_lbt)
                        else:
                            pass

                        case_equip = UPDATE_CASE_EQUIPMENT_MAPPING.format(equipment[0])
                        cursor.execute(case_equip)
                        insert_case = INSERT_CASE_EQUIP_MAPPING.format(equipment[0], performance_case_name)
                        cursor.execute(insert_case)
                    except Exception as err:
                        log_error("The Exception is" + str(err))
                else:
                    print('The Function Done')
                try:
                    conn.commit()

                except Exception as commit_err:
                    log_error(commit_err)

                if conn:
                    cursor.close()
                    conn.close()
            return 0

        except AssertionError as e:
            log_error('Exception due to update_equip Function: %s' + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def get_update_equip_query(self):

        if self.query_params:
            try:
                equipment = self.query_params[EQUIPMENT].split(",")
                body = self.req_body
                feed_name = self.query_params[FEED_NAME]
                is_active = self.query_params[IS_ACTIVE]

                self.update_equip(body, equipment, feed_name, is_active)
            except Exception as e:
                log_error('Exception due to get_update_equip_query Function: %s' + str(e))

            return JsonResponse({"message": "Updated Successfully"}, status=200, safe=False)
        else:
            return JsonResponse({"message": "Error in updating"}, status=404, safe=False)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def put_lbt_equipment_update(request):
    """
    :param request: request django object
    :param equipment: equipment name will be provided
    :param feed_name: feed name will be provided
    :param is_active: is_active name will be provided
    :return: json response
    """
    obj = query_params = None
    try:

        query_params = {
            EQUIPMENT: request.GET[EQUIPMENT],
            FEED_NAME: request.GET[FEED_NAME],
            IS_ACTIVE: request.GET[IS_ACTIVE]

        }

    except:
        pass

    try:
        if request.method == PUT_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = lbt_equipment_update(query_params, json.loads(request.body))
                return obj.get_update_equip_query()
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
