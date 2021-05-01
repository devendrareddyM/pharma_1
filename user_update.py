"""
File                :   user_update.py

Description         :   This file will have user updation program

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   21/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import traceback
from datetime import datetime

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import MESSAGE_KEY, EXCEPTION_CAUSE, USEREMAIL_KEY, UPDATE_ERROR, UPDATED_SUCCESSFULLY, \
    PUT_REQUEST, ADDRESS_KEY, PHONE_KEY, GENDER_KEY, AGE_KEY, USERLASTNAME_KEY, USERMIDDLENAME_KEY, USERFIRSTNAME_KEY, \
    METHOD_NOT_ALLOWED, EMAIL_NOT_REGISTERED, STATUS_KEY, DB_ERROR, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST, HTTP_405_METHOD_NOT_ALLOWED, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation, _RequestValidation


class UpdateUser(_PostGreSqlConnection):
    """
    This class will help to update details for the existing user
    """

    def __init__(self, loggedin_user_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_user_details: loggedin userid details
        :param request_payload: request payload
        """
        super().__init__()

        self.loggedin_userid_details = loggedin_user_details
        self._request_payload = request_payload

    def update_user(self):
        """
        This function will update the user details
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__update_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s"+str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __update_user_query(self):
        """
        This function will execute the query for updating the details for the requested user
        :return: Json object
        """

        self._psql_session.execute(
            "select * from UsersManagement where email_id='{}'".format(self._request_payload[USEREMAIL_KEY]))
        result_set = self._psql_session.fetchall()

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: EMAIL_NOT_REGISTERED}, status=HTTP_400_BAD_REQUEST)

        s = pd.DataFrame(result_set)
        userid = s['id'].iloc[0]

        self._psql_session.execute(
            "update users set first_name='{}',middle_name='{}',last_name='{}',mobile='{}',age={},address='{}',status={}"
            "gender='{}',updated_at='{}',email_id= '{}' where id={}".format(
                self._request_payload[USERFIRSTNAME_KEY], self._request_payload[USERMIDDLENAME_KEY],
                self._request_payload[USERLASTNAME_KEY], self._request_payload[PHONE_KEY],
                self._request_payload[AGE_KEY], self._request_payload[ADDRESS_KEY],self._request_payload[STATUS_KEY],self._request_payload[GENDER_KEY],
                datetime.now()
                , self._request_payload[USEREMAIL_KEY], userid))

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: UPDATE_ERROR}, status=HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY})

    def __is_user_not_authorised(self):
        """
        This will query  , whether the person who is updating the user is autheticated to update the user or not
        :return: boolean object
        """
        """ Future Use"""
        # self._psql_session.execute(CHECK_USER_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGGEDINUSERID_KEY]))
        # result_set = self._psql_session.fetchone()

        # if result_set[ROLE_KEY] == ADMIN_VAL:
        #     return False
        # return True

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def update_user(request):
    """
    This function will update the existing user
    :param request: request django object
    :return: jsonobject
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)

            r_name = loggedin_user_details['role']
            names = ['superadmin', 'admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [
                        USERFIRSTNAME_KEY,
                        USERMIDDLENAME_KEY,
                        USERLASTNAME_KEY,
                        AGE_KEY,
                        GENDER_KEY,
                        USEREMAIL_KEY,
                        PHONE_KEY,
                        ADDRESS_KEY,
                        STATUS_KEY
                    ])

                    obj = UpdateUser(loggedin_user_details, request_payload)

                    return obj.update_user()
            else:
                return JsonResponse({MESSAGE_KEY: "Bad request"}, status=HTTP_400_BAD_REQUEST)

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED}, status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
