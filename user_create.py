"""
File                :   user_create.py

Description         :   This will contain all the user creating programs

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   21/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import numpy
from psycopg2.extensions import register_adapter, AsIs

import pandas as pd
from django.db import transaction, IntegrityError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from psycopg2.errorcodes import INTERNAL_ERROR
from psycopg2.extras import *

from UsersManagement.user_get import NOT_AUTHORISED
from utilities.Constants import POST_REQUEST, METHOD_NOT_ALLOWED, USERFIRSTNAME_KEY, USERMIDDLENAME_KEY, \
    USERLASTNAME_KEY, MESSAGE_KEY, PHONE_KEY, ADDRESS_KEY, GENDER_KEY, USERPASSWORD_KEY, AGE_KEY, USEREMAIL_KEY, \
    EXCEPTION_CAUSE, STATUS_KEY, CREATED_SUCCESSFULLY, \
    DB_ERROR, ROLE_ID, LOGGEDINUSERID_KEY, ROLE_KEY, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.HashingManagement import HashingSalting
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST, HTTP_405_METHOD_NOT_ALLOWED, \
    HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from Database.Authentiction_tokenization import _PostGreSqlConnection, _RequestValidation, _TokenValidation


class CreateUser(_PostGreSqlConnection):
    """
    This class will help to create or add the new user
    """

    def __init__(self, loggedin_user_details, request_payload):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_user_details: loggedin details
        :param request_payload: request payload
        """
        super().__init__()
        self.loggedin_user_details = loggedin_user_details
        self._request_payload = request_payload

    def add_user(self):
        """
        This function will add the user details
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            if self.__is_user_not_authorised():
                return JsonResponse({MESSAGE_KEY: NOT_AUTHORISED}, status=HTTP_401_UNAUTHORIZED)

            return self.__add_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s"+str (e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]}, status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(traceback.format_exc())},
                                status=HTTP_500_INTERNAL_SERVER_ERROR)

    @transaction.atomic
    def __add_user_query(self):
        """
        This function will execute the query for creating a new user
        :return: Json paylaod
        """
        self._psql_session.execute("select email_id from users")
        email = self._psql_session.fetchall()
        e_i = pd.DataFrame(email)
        em = list(e_i["email_id"])
        if self._request_payload[USEREMAIL_KEY] not in em:
            try:
                with transaction.atomic():
                    obj = HashingSalting()
                    hash_value, salt_value = obj.get_hashed_password(self._request_payload[USERPASSWORD_KEY])
                    self._psql_session.execute(
                        "INSERT INTO users (first_name, middle_name,last_name, email_id,password, salt, "
                        "gender, age, "
                        "mobile, address,status,created_at)VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',"
                        "'{}', "
                        "now())".format(
                            self._request_payload[USERFIRSTNAME_KEY],
                            self._request_payload[USERMIDDLENAME_KEY],
                            self._request_payload[USERLASTNAME_KEY],
                            self._request_payload[USEREMAIL_KEY],
                            hash_value, salt_value,
                            self._request_payload[GENDER_KEY],
                            self._request_payload[AGE_KEY],
                            self._request_payload[PHONE_KEY],
                            self._request_payload[ADDRESS_KEY],
                            self._request_payload[STATUS_KEY]
                        ))
                    self._psql_session.execute(
                        "select * from users where email_id='{}'".format(self._request_payload[USEREMAIL_KEY]))
                    result_set = self._psql_session.fetchall()
                    s = pd.DataFrame(result_set)
                    userid = s['id'].iloc[0]
                    self._psql_session.execute(
                        "insert into role_users(role_id,user_id,created_at)VALUES({},{},now())".format(
                            self._request_payload[ROLE_ID], userid))
                    self._psql_session.execute("select permission_id from role_permission where role_id={}".format(
                        self._request_payload[ROLE_ID]))
                    result = self._psql_session.fetchall()
                    p = pd.DataFrame(result)
                    pid = list(p['permission_id'])
                    data = []
                    for i in pid:
                        data.append((userid, i))
                    execute_values(self._psql_session, "INSERT INTO user_permission (user_id,permission_id) VALUES %s",
                                   data)
                    return JsonResponse({MESSAGE_KEY: CREATED_SUCCESSFULLY})

            except IntegrityError as e:
                log_error("Exception due to : %s" + str(e))
                return JsonResponse({MESSAGE_KEY: traceback.format_exc()},
                                    status=HTTP_500_INTERNAL_SERVER_ERROR)

            except Exception as e:
                log_error("Exception due to : %s" + str(e))
                return JsonResponse({MESSAGE_KEY: INTERNAL_ERROR.format(traceback.format_exc())},
                                    status=HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return JsonResponse({MESSAGE_KEY: "Email is all ready registered"})

    def __is_user_not_authorised(self):
        """
        This will query  , whether the person who is adding the user is authenticated to add the user or not
        :return: boolean object
        """
        """" Future use """
        # self._psql_session.execute(
        #     CHECK_USER_AUTHENTICATION_QUERY.format(self.loggedin_user_details[LOGGEDINUSERID_KEY]))
        # result_set = self._psql_session.fetchone()
        # if self._psql_session.rowcount and result_set[ROLE_KEY] == ADMIN_VAL:
        #     return False
        # return True

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def create_user(request):
    """
    This function will crete a new user
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == POST_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)

            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['superadmin', 'admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [
                        USERFIRSTNAME_KEY,
                        USERMIDDLENAME_KEY,
                        USERLASTNAME_KEY,
                        USEREMAIL_KEY,
                        USERPASSWORD_KEY,
                        GENDER_KEY,
                        AGE_KEY,
                        PHONE_KEY,
                        ADDRESS_KEY,
                        STATUS_KEY,
                        ROLE_ID

                    ])
                    if jwt_value:
                        obj = CreateUser(loggedin_user_details, request_payload)
                        return obj.add_user()
                    else:
                        return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
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


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)
