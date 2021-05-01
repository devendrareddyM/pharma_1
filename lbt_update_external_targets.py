"""
File                :   lbt_udpate_external_targets.py

Description         :   This file will have user updation program

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   21/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import parser
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

# from utilities.Constants import PUT_REQUEST, USERNAME_KEY, USERFIRSTNAME_KEY, USERMIDDLENAME_KEY, USERLASTNAME_KEY, \
#     AGE_KEY, GENDER_KEY, COMMISSIONSTATUS_KEY, ROLE_KEY, ADDRESS_KEY, PHONE_KEY, METHOD_NOT_ALLOWED, MESSAGE_KEY, \
#     STATUS_KEY, EXCEPTION_CAUSE, USERNAME_NOT_REGISTERED, USERID_KEY, UPDATE_ERROR, UPDATED_SUCCESSFULLY, USEREMAIL_KEY, \
#     EMAIL_NOT_REGISTERED
from utilities.Constants import MESSAGE_KEY, EXCEPTION_CAUSE, UPDATE_ERROR, UPDATED_SUCCESSFULLY, \
    PUT_REQUEST, METHOD_NOT_ALLOWED, STATUS_KEY, DB_ERROR, EQUIPMENT, TAG_NAME_REQUEST, COMMENT_NAME, SET_LIMIT, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
# from utilities.Queries import CHECK_USER_EXISTANCE_QUERY, UPDATE_USER_QUERY, UPDATE_USER_CONFIG_QUERY, \
#     CHECK_USER_AUTHENTICATION_QUERY
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import UPDATE_EXTERNAL_TARGET
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST, HTTP_405_METHOD_NOT_ALLOWED, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation, _RequestValidation


class lbt_update_external_targets(_PostGreSqlConnection):
    """
    This ApplicationInterface used for updating the comments for external target overview page for that selecting particular equipment
    """

    def __init__(self, query_params =None, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param query_params: query_params
        :param request_payload: request payload
        """
        super().__init__()
        self.query_params = query_params
        self._request_payload = request_payload


    def update_external_targets(self):
        """
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__update_external_query()

        except AssertionError as e:
            log_error("Exception due to update_external_targets Function: %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __update_external_query(self):

        if len(self._request_payload[COMMENT_NAME]) <= 150:
            result_set = self._psql_session.execute(
                UPDATE_EXTERNAL_TARGET.format(self._request_payload[COMMENT_NAME],
                                                     self.query_params[EQUIPMENT],
                                             self.query_params[TAG_NAME_REQUEST]))
        else:
            return JsonResponse({MESSAGE_KEY: SET_LIMIT})




        s = pd.DataFrame(result_set)

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: UPDATE_ERROR}, status=HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY})

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_lbt_update_external_targets(request):
    """
    This function will update the existing user
    :param request: request django object
    :return: jsonobject
    """

    obj = query_params = None


    try:

        query_params = {
            EQUIPMENT: request.GET[EQUIPMENT],
            TAG_NAME_REQUEST: request.GET[TAG_NAME_REQUEST]
        }

    except:
        pass


    try:
        if request.method == PUT_REQUEST:

            loggedin_external_details = _TokenValidation.validate_token(request)

            r_name = loggedin_external_details['role']
            names = ['superadmin', 'admin']
            if r_name in names:
                if loggedin_external_details:

                    request_payload = _RequestValidation().validate_request(request, [COMMENT_NAME])

                    obj = lbt_update_external_targets(query_params, request_payload)


                    return obj.update_external_targets()
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
