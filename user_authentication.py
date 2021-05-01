"""
File                :   user_authentication 

Description         :   This file will contain the user login handle

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   21/2/19

Date Modified       :   5/12/2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import traceback
import datetime

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database.Configuration import ENV
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, POST_REQUEST, METHOD_NOT_ALLOWED, USERPASSWORD_KEY, \
    USEREMAIL_KEY, SALT_KEY, LOGGEDINUSERID_KEY, USERID_KEY, \
    TOKEN_KEY, PASSWORD_WRONG, USERNAME_NOT_REGISTERED, EXCEPTION_CAUSE, LOGGEDINUSEREMAIL_KEY, STATUS_VALUE, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.HashingManagement import HashingSalting
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import USER_AUTHETICATION_QUERY, USER_PROD_AUTHETICATION_QUERY
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_401_UNAUTHORIZED, HTTP_405_METHOD_NOT_ALLOWED, \
    HTTP_403_FORBIDDEN
from utilities.TokenManagement import TokenManagement
from Database.Authentiction_tokenization import _PostGreSqlConnection, _RequestValidation, _TokenValidation


class UserAuthentication(_PostGreSqlConnection):
    """
    This class is responsible for authenticating the user
    """

    def __init__(self, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param request_payload: request payload
        """
        super().__init__()
        self._request_payload = request_payload

    def handle_login(self):
        """
        This will get query from the Database for the username and validation
        :return: Json Response
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            if ENV:
                self._psql_session.execute(USER_AUTHETICATION_QUERY.format(self._request_payload[USEREMAIL_KEY]))
            else:
                self._psql_session.execute(USER_PROD_AUTHETICATION_QUERY.format(self._request_payload[USEREMAIL_KEY]))

            result_set = self._psql_session.fetchone()
            if result_set:

                obj = HashingSalting()
                if obj.check_password(self._request_payload[USERPASSWORD_KEY], result_set[SALT_KEY],
                                      result_set[USERPASSWORD_KEY]):

                    if not result_set['status']:
                        return JsonResponse({MESSAGE_KEY: STATUS_VALUE}, status=HTTP_401_UNAUTHORIZED)

                    self._psql_session.execute(
                        "select permission_name, role_name, first_name, last_name from users inner join role_users on "
                        "role_users.user_id "
                        "=users.id inner join role on role.id = role_users.role_id inner join user_permission on "
                        "user_permission.user_id = users.id inner join permission on permission.id = "
                        "user_permission.permission_id where users.email_id='{}'".format(result_set[USEREMAIL_KEY]))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    jwt_token = None
                    if not df.empty:
                        jwt_token = TokenManagement().add_jwt(
                            {
                                LOGGEDINUSERID_KEY: result_set[USERID_KEY],
                                LOGGEDINUSEREMAIL_KEY: result_set[USEREMAIL_KEY],
                                "permission": list(df["permission_name"]),
                                "role": str(df["role_name"].iloc[0]),
                                "first_name": str(df["first_name"].iloc[0]),
                                "last_name": str(df["last_name"].iloc[0]),
                                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=86400)
                            }
                        )

                    return JsonResponse({TOKEN_KEY: jwt_token})

                return JsonResponse({MESSAGE_KEY: PASSWORD_WRONG}, status=HTTP_401_UNAUTHORIZED)
            return JsonResponse({MESSAGE_KEY: USERNAME_NOT_REGISTERED}, status=HTTP_401_UNAUTHORIZED)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def authenticate_user(request):
    """
    This function will validate the user and on successful response it will generate the JWT token
    :param request: request django object
    :return: json response
    """

    obj = None

    try:
        if request.method == POST_REQUEST:
                request_payload = _RequestValidation().validate_request(request, [USEREMAIL_KEY, USERPASSWORD_KEY])
                obj = UserAuthentication(request_payload)
                return obj.handle_login()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED}, status=HTTP_405_METHOD_NOT_ALLOWED)

    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
