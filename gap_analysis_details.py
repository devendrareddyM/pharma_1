"""
File                :   inferential_logic.py

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation
from Database.db_queries import max_production_overview, min_sp_energy_overview, TOTAL_MARGIN_CHART_DATA, BREAK_UP_DATA, \
    RAW_MATERIAL_DATA, TOTAL_PRODUCT_DATA, TOTAL_ENERGY_DATA, GAP_ANALYSIS_RESULT_OTHERS_DATA, CONFIG_MSG_DATA, \
    GAP_ANALYSIS_RESULT_OTHERS_CHART_1_DATA, GAP_ANALYSIS_RESULT_OTHERS_CHART_2_DATA, \
    GAP_ANALYSIS_RESULT_OTHERS_SUCTION_PRESSURE_DATA, GAP_ANALYSIS_RESULT_OTHERS_RPM_DATA
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from utilities.http_request import error_instance


class gap_analysis_overview_details(_PostGreSqlConnection):
    """
    This ApplicationInterface gives the all furnaces names along with id particularly for hot console 1
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        :param furnaces: request payload
        """
        super().__init__()

    def get_overview_details_data(self, parameter=None):
        """
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            dict_data = {}
            match_condition_data = None
            result = []
            KPI = None
            try:
                """ Getting the  Max_production_current ts,historical best,ambient condition,feed slate"""
                dict_data["Match_Condition"] = {}
                try:
                    if parameter == 'production':
                        KPI = 'Total_Margin_Total_Production_Gap'
                        self._psql_session.execute(max_production_overview)
                    else:
                        KPI = 'Total_Margin_Total_energy_Gap'
                        self._psql_session.execute(min_sp_energy_overview)
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        match_condition_data = df
                        dict_data["Match_Condition"] = {
                            "feed_slate": match_condition_data.feed_slate.iloc[0],
                            "ambient_condition": match_condition_data.ambient_condition.iloc[0],
                            "timestamp": match_condition_data.timestamp.iloc[0],
                            "hystorical_best": match_condition_data.hystorical_best.iloc[0],
                        }
                    else:
                        dict_data["Match_Condition"] = match_condition_data
                except Exception as e:
                    log_error('Exception in Match_Condition_data: %s' + str(e))
                dict_data["Overall Gap"] = []
                try:
                    self._psql_session.execute(TOTAL_MARGIN_CHART_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Overall Gap"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Overall Gap"] = []
                except Exception as e:
                    log_error('Exception in overall gap data: %s' + str(e))

                dict_data["Breakup"] = []
                try:
                    self._psql_session.execute(BREAK_UP_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Breakup"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Breakup"] = []
                except Exception as e:
                    log_error('Exception in Break up data: %s' + str(e))

                dict_data["Raw Material"] = []
                try:
                    self._psql_session.execute(RAW_MATERIAL_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Raw Material"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Raw Material"] = []
                except Exception as e:
                    log_error('Exception in Raw Material data: %s' + str(e))

                dict_data["Product"] = []
                try:
                    self._psql_session.execute(TOTAL_PRODUCT_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Product"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Product"] = []
                except Exception as e:
                    log_error('Exception in Product data: %s' + str(e))
                dict_data["Energy"] = []
                try:
                    self._psql_session.execute(TOTAL_ENERGY_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Energy"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Energy"] = []
                except Exception as e:
                    log_error('Exception in Energy data: %s' + str(e))
                dict_data["Others"] = {}
                try:
                    self._psql_session.execute(GAP_ANALYSIS_RESULT_OTHERS_CHART_1_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Others"]["Chart_1"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Others"]["Chart_1"] = []
                except Exception as e:
                    log_error('Exception in Chart_1 data: %s' + str(e))
                try:
                    self._psql_session.execute(GAP_ANALYSIS_RESULT_OTHERS_CHART_2_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Others"]["Chart_2"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Others"]["Chart_2"] = []
                except Exception as e:
                    log_error('Exception in Chart_2 data: %s' + str(e))
                try:
                    self._psql_session.execute(GAP_ANALYSIS_RESULT_OTHERS_SUCTION_PRESSURE_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Others"]["Chart_3"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Others"]["Chart_3"] = []
                except Exception as e:
                    log_error('Exception in Chart_3 data: %s' + str(e))
                try:
                    self._psql_session.execute(GAP_ANALYSIS_RESULT_OTHERS_RPM_DATA.format(parameter))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["Others"]["Chart_4"] = yaml.safe_load(
                            df.to_json(orient="records"))
                    else:
                        dict_data["Others"]["Chart_4"] = []
                except Exception as e:
                    log_error('Exception in Chart_4 data: %s' + str(e))
                try:
                    self._psql_session.execute(CONFIG_MSG_DATA.format(KPI))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data["config_message"] = df.tag_value_txt.iloc[0]
                    else:
                        dict_data["config_message"] = None
                except Exception as e:
                    log_error('Exception in Energy data: %s' + str(e))
                # df = pd.read_csv('August.csv', encoding='unicode_escape')
                # rslt_df = df[df['tag_type'] == 'Input']
                # rslt_df.to_csv("Aug_in.csv",index=False)

            except Exception as e:
                log_error('Exception due to get_overview_details_data Function: %s' + str(e))
            if dict_data:
                result.append(dict_data)
            return JsonResponse(result, safe=False)
        except AssertionError as e:
            log_error('Exception in details api Function: %s' + str(e))
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
def get_gap_analysis_overview_details_data(request):
    """
    :param request: request django object
    :return: json response
    """
    obj = None

    obj = query_params = None
    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            parameter = str(request.GET.get("basis"))
            if jwt_value:
                obj = gap_analysis_overview_details()
                return obj.get_overview_details_data(parameter)
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
