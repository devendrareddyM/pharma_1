"""
File                :   lbt_external_targets.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, TIMESTAMP_KEY, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import GET_EXTERNAL_TARGET_RESULT
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_400_BAD_REQUEST, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class lbt_external_targets(_PostGreSqlConnection):
    """
    This ApplicationInterface shows all the furnaces data based on every 10 min after completion algo run
    """

    def __init__(self, external=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param external: it will be provided
        """
        super().__init__()
        self.external = external


    def get_externl_targets(self):
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")

            timetsamp_record = self._psql_session.fetchone()
            if not timetsamp_record:
                return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )

            self.sync_time = timetsamp_record[TIMESTAMP_KEY]


            self._psql_session.execute(GET_EXTERNAL_TARGET_RESULT)

            df = pd.DataFrame(self._psql_session.fetchall())


            if df.shape[0]:
                df = df.where(pd.notnull(df) == True, None)
                console_name = df["console_name"].unique()
                unit_val = {}
                final_val = []
                console_val = []
                for console in console_name:
                    equipment_val = {"console_name": console,
                                     "equipments": df[["equipment_tag_name", "min", "max", "target", "actual","feed_type",
                                                       "comment", "description", "status", "alert_flag", "tag" ,"equipment_id"]][
                                         (df["console_name"] == console)].to_dict(
                                         orient=RECORDS)}

                    console_val.append(equipment_val)

                unit_val = console_val
                final_val.append(unit_val)

                return JsonResponse(final_val,
                                        safe=False)
            else:
                return JsonResponse([],
                                safe=False)

        except AssertionError as e:
            log_error('Exception due to get_externl_targets Function: %s' + str(e))
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
def get_lbt_external_targets(request):
    """
    :param request: request django object
    :return: json response
    """
    obj = None
    external = None

    try:
        if request.method == GET_REQUEST:
            loggedin_external_details = _TokenValidation.validate_token(request)

            r_name = loggedin_external_details['role']
            names = ['superadmin', 'admin','operator']
            if r_name in names:
                if loggedin_external_details:
                    obj = lbt_external_targets(external)
                    return obj.get_externl_targets()
            else:
                return JsonResponse({MESSAGE_KEY: "Bad request"}, status=HTTP_400_BAD_REQUEST)
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
