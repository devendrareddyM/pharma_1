"""
File                :   inferential_logic.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback
import json
import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation
from Database.db_queries import max_production_overview, min_sp_energy_overview, max_production_total_margin, \
    max_production_total_raw_material, max_production_product_data, max_production_energy_data, \
    min_sp_energy_total_margin, min_sp_energy_total_raw_material, min_sp_energy_product_data, min_sp_energy_energy_data, \
    max_production_config_msg_data, min_sp_energy_config_msg_data
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from utilities.http_request import error_instance


class gap_analysis_overview(_PostGreSqlConnection):
    """
    This ApplicationInterface gives the all furnaces names along with id particularly for hot console 1
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        :param furnaces: request payload
        """
        super().__init__()

    def get_sanky_output(self):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            result = []
            max_production_overview_data = None
            min_spec_energy_overview_data = None
            config_msg = None
            dict_data = {}
            try:
                """ Getting the  Max_production_current ts,historical best,ambient condition,feed slate"""
                dict_data["max_production_overview"] = {}
                try:
                    self._psql_session.execute(max_production_overview)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        max_production_overview_data = df
                        dict_data["max_production_overview"] = {
                            "feed_slate": max_production_overview_data.feed_slate.iloc[0],
                            "ambient_condition": max_production_overview_data.ambient_condition.iloc[0],
                            "timestamp": max_production_overview_data.timestamp.iloc[0],
                            "hystorical_best": max_production_overview_data.hystorical_best.iloc[0],
                        }
                    else:
                        dict_data["max_production_overview"] = {}
                except Exception as e:
                    log_error('Exception in max_production_overview_data: %s' + str(e))

                """ ADDING CONFIG_MESSAGE """

                try:
                    self._psql_session.execute(max_production_config_msg_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        config_msg = df
                        dict_data["max_production_overview"]["config_message"] = config_msg.tag_value_txt.iloc[0]
                    else:
                        dict_data["max_production_overview"]["config_message"] = None
                except Exception as e:
                    log_error('Exception in max_production_overview_data_config_message: %s' + str(e))

                """Getting minimum energy current ts,historical best,ambient condition,feed slate data"""

                dict_data["min_spec_energy_overview"] = {}
                try:
                    self._psql_session.execute(min_sp_energy_overview)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        min_spec_energy_overview_data = df
                        dict_data["min_spec_energy_overview"] = {
                            "feed_slate": min_spec_energy_overview_data.feed_slate.iloc[0],
                            "ambient_condition": min_spec_energy_overview_data.ambient_condition.iloc[0],
                            "timestamp": min_spec_energy_overview_data.timestamp.iloc[0],
                            "hystorical_best": min_spec_energy_overview_data.hystorical_best.iloc[0]
                        }

                    else:
                        dict_data["min_spec_energy_overview"] = {}

                except Exception as e:
                    log_error('Exception in min_spec_energy_overview: : %s' + str(e))
                try:
                    self._psql_session.execute(min_sp_energy_config_msg_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        config_msg = df
                        dict_data["min_spec_energy_overview"]["config_message"] = config_msg.tag_value_txt.iloc[0]
                    else:
                        dict_data["min_spec_energy_overview"]["config_message"] = None
                except Exception as e:
                    log_error('Exception in min_spec_energy_overview_data_config_message: %s' + str(e))

                """ Getting the  Max_production total margin data"""

                dict_data["max_production_data"] = {}
                try:
                    self._psql_session.execute(max_production_total_margin)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["max_production_data"]["Total Margin"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["max_production_data"]["Total Margin"] = []
                except Exception as e:
                    log_error('Exception in max_production_data total_margin: %s' + str(e))

                """TOTAL RAW MATERIAL"""
                try:
                    self._psql_session.execute(max_production_total_raw_material)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["max_production_data"]["Total Raw Material"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["max_production_data"]["Total Raw Material"] = []
                except Exception as e:
                    log_error('Exception in max_production_data total_raw_material: %s' + str(e))

                """PRODUCT"""

                try:
                    self._psql_session.execute(max_production_product_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["max_production_data"]["Product"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["max_production_data"]["Product"] = []
                except Exception as e:
                    log_error('Exception in max_production_data product data: %s' + str(e))

                """Max_product_data energy data"""

                try:
                    self._psql_session.execute(max_production_energy_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["max_production_data"]["Energy"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["max_production_data"]["Energy"] = []
                except Exception as e:
                    log_error('Exception in max_production_data energy data: %s' + str(e))

                """Getting min_spec_energy_data"""

                dict_data["min_spec_energy_data"] = {}

                try:
                    self._psql_session.execute(min_sp_energy_total_margin)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["min_spec_energy_data"]["Total Margin"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["min_spec_energy_data"]["Total Margin"] = []
                except Exception as e:
                    log_error('Exception in min_spec_energy_data total_margin: %s' + str(e))

                """TOTAL RAW MATERIAL"""
                try:
                    self._psql_session.execute(min_sp_energy_total_raw_material)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["min_spec_energy_data"]["Total Raw Material"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["min_spec_energy_data"]["Total Raw Material"] = []
                except Exception as e:
                    log_error('Exception in min_spec_energy_data total_raw_material: %s' + str(e))

                """min_spec_energy_data PRODUCT"""

                try:
                    self._psql_session.execute(min_sp_energy_product_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["min_spec_energy_data"]["Product"] = yaml.safe_load(
                            df.to_json(orient="records"))

                    else:
                        dict_data["min_spec_energy_data"]["Product"] = []
                except Exception as e:
                    log_error('Exception in min_spec_energy_data product data: %s' + str(e))

                """min_spec_energy_data energy data"""

                try:
                    self._psql_session.execute(min_sp_energy_energy_data)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["min_spec_energy_data"]["Energy"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["min_spec_energy_data"]["Energy"] = []
                except Exception as e:
                    log_error('Exception in min_spec_energy_data energy data: %s' + str(e))
                result.append(dict_data)
            except Exception as e:
                log_error('Exception due to get_sanky_output Function: %s' + str(e))
            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error('Exception in get_sanky_output Function: %s' + str(e))
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
def get_gap_analysis_overview(request):
    """
    :param request: request django object
    :return: json response
    """
    obj = None

    obj = query_params = None
    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = gap_analysis_overview()
                return obj.get_sanky_output()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError as e:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
