"""
File                :   user_password_change

Description         :   This will change the user's password

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   22/2/19

Date Modified       :   5/12/2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database.Configuration import DEVELOPMENT, ENV
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, EXCEPTION_CAUSE, PUT_REQUEST, USERPASSWORD_KEY, \
    USERFUTUREPASSWORD_KEY, METHOD_NOT_ALLOWED, LOGGEDINUSEREMAIL_KEY, USERNAME_NOT_REGISTERED, PASSWORD_WRONG, \
    CHANGED_SUCCESSFULLY, SALT_KEY, LOGGEDINUSERID_KEY, USERID_KEY, RESET_ERROR, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.HashingManagement import HashingSalting
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import USER_AUTHETICATION_QUERY, CHANGE_RESET_USER_PASSWORD_QUERY, \
    USER_PROD_AUTHETICATION_QUERY, \
    CHANGE_RESET_USER_PROD_PASSWORD_QUERY
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_400_BAD_REQUEST, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation, _RequestValidation


class UserPasswordChange(_PostGreSqlConnection):
    """
    This class will help to change the user's password for the existing user
    """

    def __init__(self, loggedin_userid_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: logged in details
        :param request_payload: request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self._request_payload = request_payload

    def change_password_user(self):
        """
        This function will change the user details
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            return self.__change_user_password_query()

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __change_user_password_query(self):
        """
        This function will execute the query for change the user password
        :return: Json object
        """
        if DEVELOPMENT:
            self._psql_session.execute(
                USER_AUTHETICATION_QUERY.format(self.loggedin_userid_details[LOGGEDINUSEREMAIL_KEY]))
        else:
            self._psql_session.execute(
                USER_PROD_AUTHETICATION_QUERY.format(self.loggedin_userid_details[LOGGEDINUSEREMAIL_KEY]))

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: USERNAME_NOT_REGISTERED}, status=HTTP_400_BAD_REQUEST)

        obj = HashingSalting()
        result_set = self._psql_session.fetchone()

        if obj.check_password(self._request_payload[USERPASSWORD_KEY], result_set[SALT_KEY],
                              result_set[USERPASSWORD_KEY]):

            hash_value, salt_value = obj.get_hashed_password(self._request_payload[USERFUTUREPASSWORD_KEY])

            if ENV:

                self._psql_session.execute(CHANGE_RESET_USER_PASSWORD_QUERY.format(SALT_KEY, salt_value,
                                                                                   USERPASSWORD_KEY, hash_value,
                                                                                   USERID_KEY,
                                                                                   self.loggedin_userid_details[
                                                                                       LOGGEDINUSERID_KEY]))
            else:
                self._psql_session.execute(CHANGE_RESET_USER_PROD_PASSWORD_QUERY.format(SALT_KEY, salt_value,
                                                                                        USERPASSWORD_KEY, hash_value,
                                                                                        USERID_KEY,
                                                                                        self.loggedin_userid_details[
                                                                                            LOGGEDINUSERID_KEY]))
            if self._psql_session.rowcount:
                return JsonResponse({MESSAGE_KEY: CHANGED_SUCCESSFULLY})

            return JsonResponse({MESSAGE_KEY: RESET_ERROR}, status=HTTP_400_BAD_REQUEST)

        return JsonResponse({MESSAGE_KEY: PASSWORD_WRONG}, status=HTTP_400_BAD_REQUEST)


@csrf_exempt
def user_password_change(request):
    """
    This function will reset the password for the existing user
    :param request: request django object
    :return: json object
    """
    obj = None

    try:
        if request.method == PUT_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)

            if loggedin_user_details:
                request_payload = _RequestValidation().validate_request(request,
                                                                        [USERPASSWORD_KEY, USERFUTUREPASSWORD_KEY])

                obj = UserPasswordChange(loggedin_user_details, request_payload)
                return obj.change_password_user()

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
