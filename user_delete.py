"""
File                :   user_delete.py

Description         :   This file will handle the delete operations

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   21/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, USEREMAIL_KEY, DELETE_REQUEST, DELETED_SUCCESSFULLY, \
    DELETED_ERROR, EXCEPTION_CAUSE, STATUS_KEY, EMAIL_NOT_REGISTERED, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import *

from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation, _RequestValidation


class DeleteUser(_PostGreSqlConnection):
    """
    This class will help to delete the existing user
    """

    def __init__(self, loggedin_userid_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: loggedin userid
        :param username: username that is need to be deleted
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self._request_payload = request_payload

    def delete_user(self):
        """
        This function will delete the user details
        :return: Json payload
        """
        try:

            return self.__delete_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s"+ str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __delete_user_query(self):
        """
        This function will execute the query for deleting the requested user
        :return: Json object
        """

        self._psql_session.execute(
            "select * from UsersManagement where email_id='{}'".format(self._request_payload[USEREMAIL_KEY]))
        result_set = self._psql_session.fetchall()
        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: EMAIL_NOT_REGISTERED}, status=HTTP_400_BAD_REQUEST)

        s = pd.DataFrame(result_set)
        userid = s['id'].iloc[0]
        self._psql_session.execute("DELETE from users where id={}".format(userid))
        self._psql_session.execute("DELETE from role_users where user_id={}".format(userid))
        self._psql_session.execute("DELETE from user_permission  where user_id={}".format(userid))

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: DELETED_ERROR}, status=HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({MESSAGE_KEY: DELETED_SUCCESSFULLY})

    def __is_user_not_authorised(self):
        """
        This will query  , whether the person who is deleting the user is authenticated to delete the user or not
        :return: boolean object
        """
        pass

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def delete_user(request):
    """
    This function will delete the existing user
    :param request: request django object
    :param username : username that need to be deleted
    :return: json object
    """
    obj = None

    try:
        if request.method == DELETE_REQUEST:
            # jwt_value = _TokenValidation().validate_token(request)
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['superadmin', 'admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [USEREMAIL_KEY])
                    obj = DeleteUser(loggedin_user_details, request_payload)
                    return obj.delete_user()
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
