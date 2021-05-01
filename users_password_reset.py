"""
File                :   user_password_reset

Description         :   This file is having the programs for reset the user's password.

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   22/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from psycopg2.errorcodes import INTERNAL_ERROR

from Database.db_queries import CHANGE_RESET_USER_PASSWORD_QUERY
from utilities.Constants import MESSAGE_KEY, STATUS_KEY, DB_ERROR, USERNAME_KEY, USERNAME_NOT_REGISTERED, \
    USERPASSWORD_KEY, SALT_KEY, CHANGE_ERROR, RESET_SUCCESSFULLY, PUT_REQUEST, METHOD_NOT_ALLOWED, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.HashingManagement import HashingSalting
from utilities.LoggerFile import *
# from Database.db_queries import CHECK_USER_AUTHENTICATION_QUERY, CHECK_USER_EXISTANCE_QUERY, \
#     CHANGE_RESET_USER_PASSWORD_QUERY
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation, _RequestValidation


class UserPasswordReset(_PostGreSqlConnection):
    """
    This class will help to reset the user's password for the existing user
    """

    def __init__(self, loggedin_userid_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: logged in user id
        :param request_payload: request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self._request_payload = request_payload

    def reset_user_password(self):
        """
        This function will reset the user password
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            if self.__is_user_not_authorised():
                return JsonResponse({MESSAGE_KEY: "NOT AUTHORIZED"}, status=HTTP_401_UNAUTHORIZED)

            return self.__reset_user_password_query()

        except Exception as e:
            log_info(e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

    def __reset_user_password_query(self):
        """
        This function will execute the query for resetting the user password
        :return: Json object
        """
        try:
            with transaction.atomic():

                """ Future use """
                # self._psql_session.execute(CHECK_USER_EXISTANCE_QUERY.format(self._request_payload[USERNAME_KEY]))

                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: USERNAME_NOT_REGISTERED}, status=HTTP_400_BAD_REQUEST)

                obj = HashingSalting()
                hash, salt = obj.get_hashed_password(self._request_payload[USERPASSWORD_KEY])

                self._psql_session.execute(CHANGE_RESET_USER_PASSWORD_QUERY.format(SALT_KEY, salt,
                                                                                   USERPASSWORD_KEY, hash,
                                                                                   USERNAME_KEY,
                                                                                   self._request_payload[USERNAME_KEY]))
                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: CHANGE_ERROR}, status=HTTP_400_BAD_REQUEST)

                return JsonResponse({MESSAGE_KEY: RESET_SUCCESSFULLY})

        except IntegrityError as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: traceback.format_exc()},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

        except Exception as e:
            log_error(traceback.format_exc())

            return JsonResponse({MESSAGE_KEY: INTERNAL_ERROR.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __is_user_not_authorised(self):
        """
        This will query  , whether the person who is changing the user password is authenticated to change or not
        :return: boolean object
        """
        """ Future use"""
        # self._psql_session.execute(CHECK_USER_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGGEDINUSERID_KEY]))
        # result_set = self._psql_session.fetchone()

        # if self._psql_session.rowcount and result_set[ROLE_KEY] == ADMIN_VAL:
        #     return False

        return True

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def resetpassword_user(request, userid):
    """
    This function will reset the password for the existing user
    :param userid:
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)

            if loggedin_user_details:
                request_payload = _RequestValidation().validate_request(request, [USERNAME_KEY, USERPASSWORD_KEY])
                obj = UserPasswordReset(loggedin_user_details, request_payload)
                return obj.reset_user_password()

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
