"""
File                :   user_get.py

Description         :   This file is for getting the details for the particular username

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   1/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
from django.http import JsonResponse

from utilities.Constants import MESSAGE_KEY, EXCEPTION_CAUSE, STATUS_KEY, DB_ERROR, ROLE_KEY, LOGGEDINUSERID_KEY, \
    GET_REQUEST, METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import *
# from Database.db_queries import GET_SELECTED_USER_QUERY, CHECK_USER_AUTHENTICATION_QUERY
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


def NOT_AUTHORISED(args):
    pass


def MISSING_USERNAME(args):
    pass


class GetUserList(_PostGreSqlConnection):
    """
    This class will help to get the details for the existing users
    """

    def __init__(self, username, loggedin_userid_details):
        """
        This will call the parent class to validate the connection and request payload
        :param username: this will control the query a particular user
        :param loggedin_userid_details : loggedin userid details
        """
        super().__init__()
        self.username = username
        self.loggedin_userid_details = loggedin_userid_details

    def getuser_list(self):
        """
        This function will get the user list
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            if self.__is_user_not_authorised():
                return JsonResponse({MESSAGE_KEY: NOT_AUTHORISED}, status=HTTP_401_UNAUTHORIZED)

            return self.__get_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __get_user_query(self):
        """
        This function will execute the query for getting the details for the requested user or all users
        :return: Json object
        """

        if self.username:
            # self._psql_session.execute(GET_SELECTED_USER_QUERY.format(self.username))
            result_set = self._psql_session.fetchone()
            if result_set is None:
                result_set = []
            return JsonResponse(result_set, safe=False)

        return JsonResponse({MESSAGE_KEY: MISSING_USERNAME}, status=HTTP_400_BAD_REQUEST)

    def __is_user_not_authorised(self, ADMIN_VAL=None):
        """
        This will query  , whether the person who is updating the user is autheticated to get the user details or not
        :return: boolean object
        """

        # self._psql_session.execute(CHECK_USER_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGGEDINUSERID_KEY]))
        result_set = self._psql_session.fetchone()

        if result_set[ROLE_KEY] == ADMIN_VAL:
            return False

        return True

    def __del__(self):
        self._psql_session.close()


def get_user_list(request, username):
    """
    This function will help to get all the users
    :param request: request django object
    :param username : for particular username
    :return: json object
    """

    obj = None
    try:
        if request.method == GET_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)

            if loggedin_user_details:
                obj = GetUserList(username, loggedin_user_details)
                return obj.getuser_list()

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
