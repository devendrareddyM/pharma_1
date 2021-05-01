"""
File                :   plant_equipment_hierarchy.py

Description         :   this will return all the console name and equipment name
                        for the associated unit

Author              :   LivNSense Technologies

Date Created        :   21-08-2019

Date Last modified :    21-08-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    RECORDS, HOT_CONSOLE_1_VALUE, HOT_CONSOLE_2_VALUE, COLD_CONSOLE_1_VALUE, COLD_CONSOLE_2_VALUE, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import MASTER_TABLE_QUERY, MASTER_TABLE_QUERY_OPERATOR
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class GetNamings(_PostGreSqlConnection):
    """
    This class is responsible for getting the names for all the console and associated equipment for unit
    """

    def __init__(self, get_value):
        """
        This will call the parent class to validate the connection
        """
        super().__init__()
        self.get_value = get_value

    def get_names_values(self):
        """
        This will return the names for every unit with console and equipments
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.get_value == 0:
                self._psql_session.execute(MASTER_TABLE_QUERY)
            elif self.get_value == 1:
                self._psql_session.execute(MASTER_TABLE_QUERY_OPERATOR.format(HOT_CONSOLE_1_VALUE))
            elif self.get_value == 2:
                self._psql_session.execute(MASTER_TABLE_QUERY_OPERATOR.format(HOT_CONSOLE_2_VALUE))
            elif self.get_value == 3:
                self._psql_session.execute(MASTER_TABLE_QUERY_OPERATOR.format(COLD_CONSOLE_1_VALUE))
            elif self.get_value == 4:
                self._psql_session.execute(MASTER_TABLE_QUERY_OPERATOR.format(COLD_CONSOLE_2_VALUE))

            df = pd.DataFrame(self._psql_session.fetchall())
            if df.shape[0]:
                unit_name = df["unit_name"].unique()
                console_name = df["console_name"].unique()
                unit_val = {}
                final_val = []
                for unit in unit_name:
                    console_val = []
                    for console in console_name:
                        equipment_val = {}
                        equipment_val["console_name"] = console
                        equipment_val["equipments"] = df[["equipment_tag_name", "equipment_name", "equipment_id"]][
                            (df["console_name"] == console) & (df["unit_name"] == unit)].to_dict(
                            orient=RECORDS)
                        console_val.append(equipment_val)

                    unit_val["unit_name"] = unit
                    unit_val["consoles"] = console_val
                    final_val.append(unit_val)

                return JsonResponse(final_val,
                                    safe=False)
            return JsonResponse([],
                                safe=False)

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
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_namings_data(request):
    """
    This function will return all the console name and equipment name
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)

            get_value = 0

            if jwt_value['role'] in ['superadmin', 'admin', 'engineer']:
                get_value = 0
            elif jwt_value['loggedin_useremail'] in ['operator1@cpchem.com']:
                get_value = 1
            elif jwt_value['loggedin_useremail'] in ['operator2@cpchem.com']:
                get_value = 2
            elif jwt_value['loggedin_useremail'] in ['operator3@cpchem.com']:
                get_value = 3
            elif jwt_value['loggedin_useremail'] in ['operator4@cpchem.com']:
                get_value = 4
            if jwt_value:

                obj = GetNamings(get_value)

                return obj.get_names_values()
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
