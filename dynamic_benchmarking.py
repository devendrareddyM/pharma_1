"""
File                :   dynamic_benchmarking.py

Description         :   This file will return the dynamic benchmarking result for the particular equipments

Author              :   LivNSense Technologies

Date Created        :   18-07-2019

Date Last modified :    19-07-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import datetime
import traceback
import time as t
from datetime import datetime

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database import InputValidation
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, TIMESTAMP_KEY, \
    DYNAMIC_BENCHMARKING, FLAG_STATUS_VALUE, HOT_CONSOLE_1_VALUE, TAG_NAME_REQUEST, START_DATE_REQUEST, \
    END_DATE_REQUEST, FDHDR_TAGS, tag_list, HOT_CONSOLE_2_VALUE, \
    COLD_CONSOLE_1_VALUE, COLD_CONSOLE_2_VALUE, HOT_CONSOLE_1_EQUIPMENTS, NON_FURNACE_EQUIPMENTS, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import CONSOLE_EXCHANGER_HEALTH_EH, \
    DETAIL_LEVEL_PERFORMANCE_TAGS2, DETAIL_LEVEL_EXTERNAL_TAGS2, \
    DETAIL_LEVEL_MATCH_TAGS2, BEST_TIME_HISTORICAL_TIMESTAMP, LBT_DETAILED_GRAPH_NULL_START_DATE, LBT_DETAILED_GRAPH, \
    FDHDR_VALUE, DOL_VALUE, SHORT_NAME, DETAIL_LEVEL_NON_FURNACE_EXTERNAL_TAGS, DETAIL_LEVEL_NON_FURNACE_MATCH_TAGS, \
    DETAIL_LEVEL_NON_FURNACE_PERF_TAGS, HIST_BEST_NON_FURNACE, NON_LBT_DETAIL_GRAPH_NULL_START_DATE, \
    NON_LBT_DETAILED_GRAPH
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_404_NOT_FOUND, \
    HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class DynamicBenchmarking(_PostGreSqlConnection):
    """
    This class is responsible for reading data from the Database and perform operation according to LBT algo
    and return JSON
    """

    def __init__(self, query_params, unit=None, console=None, equipment=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.console = console
        self.equipment = equipment
        self.unit = unit
        self.query_params = query_params

    def get_data_with_is_mapped_short_name(self, df, mapped_tags, unmapped_tags, lbt_result_equip):
        """
        Map & UnMap xlsx value check
        """
        df_mapped = df[df["is_mapped"] == True]
        temp = unmapped_tags[[self.equipment]].merge(df, left_on=self.equipment, right_on="tag_name")
        temp["is_mapped"] = False
        df.update(temp)
        df.drop_duplicates(subset="tag_name",  keep="first" ,inplace=True)
        df_unmapped_check = df[df["is_mapped"] == False]

        df_None = df[df["is_mapped"].isnull()]
        df = [df_mapped, df_unmapped_check, df_None]
        result = pd.concat(df)
        df = result.merge(lbt_result_equip[['short_name', 'Tags']], how='left', left_on=['tag_name'],
                          right_on=['Tags']).fillna("")

        df.drop(columns=['Tags'], inplace=True)
        return df

    def get_values(self):
        """
        This will get query from the Database for the console and equipment name
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            self.sync_time = None
            dynamic_benchmarking_status = 0

            COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}' and feature = '{}' "

            """
            Color Coding for tabular data
            """
            try:
                self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment, DYNAMIC_BENCHMARKING))

                tabular_df = pd.DataFrame(self._psql_session.fetchall())

                if not tabular_df.empty:

                    try:
                        dynamic_benchmarking_status = int(
                            tabular_df[FLAG_STATUS_VALUE].iloc[0])

                    except Exception as e:
                        log_error(e)

            except Exception as e:
                log_error(e)

            self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")

            timetsamp_record = self._psql_session.fetchone()

            self._psql_session.execute(
                CONSOLE_EXCHANGER_HEALTH_EH.format(self.unit, self.console, self.equipment))
            if self.equipment in HOT_CONSOLE_1_EQUIPMENTS:
                self._psql_session.execute(BEST_TIME_HISTORICAL_TIMESTAMP.format(self.equipment))
            elif self.equipment in NON_FURNACE_EQUIPMENTS:
                self._psql_session.execute(HIST_BEST_NON_FURNACE.format(self.equipment))

            best = pd.DataFrame(self._psql_session.fetchall())

            if best.empty:
                best_time = None
            else:
                best_time = best["timestamp"].iloc[0]


            self.sync_time = timetsamp_record[TIMESTAMP_KEY]
            utc_time = datetime.strptime(str(self.sync_time)[:-6], "%Y-%m-%d %H:%M:%S")
            epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
            epoch_time = int(epoch_time) * 1000
            fdhr_value = None
            dol = None
            tag_name = None
            tag_value = None
            fdhdr = pd.DataFrame()
            curr_time = t.time()
            curr_time = int(curr_time - (curr_time % 60) - 120) * 1000
            for i in FDHDR_TAGS:
                if i[-2::] == self.equipment[-2::]:
                    self._psql_session.execute(FDHDR_VALUE.format(i))
                    df = self._psql_session.fetchall()
                    tag_data = pd.DataFrame(df)
                    if not tag_data.empty:
                        fdhr_value = tag_data["tag_value"].iloc[0]
                        fdhdr["tag_name"] = tag_data["tag_name"]
                        fdhdr["tag_value"] = tag_data["tag_value"]
            for i in tag_list:
                if i[2:5] == self.equipment[3:6]:
                    self._psql_session.execute(DOL_VALUE.format(i,epoch_time))
                    data = self._psql_session.fetchall()
                    dol_data = pd.DataFrame(data)
                    if not dol_data.empty:
                        dol = dol_data['tag_value'].iloc[0]
            if not fdhdr.empty:
                tag_name = fdhdr["tag_name"].iloc[0]
                tag_value = int(fdhdr["tag_value"])


            dict_data = {

                "current_date": self.sync_time,
                "status": dynamic_benchmarking_status,
                "categories": [],
                "Performance_tag": [],
                "External_targets": [],
                "Result_tag": [],
                "best_date": best_time
            }

            dynamic_benchmarking_df = pd.DataFrame()

            if self.console == HOT_CONSOLE_1_VALUE:
                dict_data['FDHDR_TAG'] = tag_name
                dict_data['FDHDR_VALUE'] = tag_value
                dict_data["DOL_VALUE"] = dol
            else:
                pass
            """For Dummy purpose for doing dynamic benchmarking for three tags"""
            if self.console == HOT_CONSOLE_1_VALUE:

                self._psql_session.execute(SHORT_NAME.format(self.equipment))
                category_df = pd.DataFrame(self._psql_session.fetchall())
                filter_category_data = yaml.safe_load(category_df.to_json(
                    orient="records"))
                dict_data["categories"] = filter_category_data


                if self.query_params:
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            LBT_DETAILED_GRAPH_NULL_START_DATE.format(self.equipment,
                                                                      self.query_params[TAG_NAME_REQUEST],
                                                                      self.query_params[END_DATE_REQUEST]))
                    else:
                        self._psql_session.execute(
                            LBT_DETAILED_GRAPH.format(self.equipment,
                                                      self.query_params[TAG_NAME_REQUEST],
                                                      self.query_params[START_DATE_REQUEST],
                                                      self.query_params[END_DATE_REQUEST]))

                    df = pd.DataFrame(self._psql_session.fetchall())
                    graph_data = []
                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]),
                                "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]),
                                "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        graph_data.append(temp)

                    return JsonResponse(graph_data, safe=False)
                if self.equipment in HOT_CONSOLE_1_EQUIPMENTS:

                    try:
                        self._psql_session.execute(DETAIL_LEVEL_EXTERNAL_TAGS2.format(self.equipment, self.equipment))
                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                            dict_data["External_targets"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    except Exception as e:
                        log_error(e)
                    try:
                        self._psql_session.execute(DETAIL_LEVEL_MATCH_TAGS2.format(self.equipment, self.equipment))

                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)

                            dict_data["Result_tag"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    except Exception as e:
                        log_error(e)

                    try:
                        self._psql_session.execute(DETAIL_LEVEL_PERFORMANCE_TAGS2.format(self.equipment, self.equipment))
                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                            dict_data["Performance_tag"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    except Exception as e:
                        log_error(e)

            elif self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE or self.console == COLD_CONSOLE_2_VALUE:
                if self.query_params:
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            NON_LBT_DETAIL_GRAPH_NULL_START_DATE.format(self.equipment,
                                                                      self.query_params[TAG_NAME_REQUEST],
                                                                      self.query_params[END_DATE_REQUEST]))
                    else:
                        self._psql_session.execute(
                            NON_LBT_DETAILED_GRAPH.format(self.equipment,
                                                      self.query_params[TAG_NAME_REQUEST],
                                                      self.query_params[START_DATE_REQUEST],
                                                      self.query_params[END_DATE_REQUEST]))

                    df = pd.DataFrame(self._psql_session.fetchall())
                    graph_data = []
                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]),
                                "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]),
                                "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        graph_data.append(temp)

                    return JsonResponse(graph_data, safe=False)

                if self.equipment in NON_FURNACE_EQUIPMENTS:

                    self._psql_session.execute(SHORT_NAME.format(self.equipment))
                    category_df = pd.DataFrame(self._psql_session.fetchall())
                    filter_category_data = yaml.safe_load(category_df.to_json(
                        orient="records"))
                    dict_data["categories"] = filter_category_data
                    try:
                        self._psql_session.execute(DETAIL_LEVEL_NON_FURNACE_EXTERNAL_TAGS.format(self.equipment))
                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                            dict_data["External_targets"] = yaml.safe_load(df.to_json(orient=RECORDS))
                    except Exception as e:
                        log_error(e)
                    try:
                        self._psql_session.execute(DETAIL_LEVEL_NON_FURNACE_MATCH_TAGS.format(self.equipment))
                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)

                            dict_data["Result_tag"] = yaml.safe_load(df.to_json(orient=RECORDS))
                    except Exception as e:
                        log_error(e)

                    try:
                        self._psql_session.execute(DETAIL_LEVEL_NON_FURNACE_PERF_TAGS.format(self.equipment))
                        df = pd.DataFrame(self._psql_session.fetchall())

                        if not df.empty:
                            df = df.where(pd.notnull(df) == True, None)
                            dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                            dict_data["Performance_tag"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    except Exception as e:
                        log_error(e)
            else:
                pass


            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error(e)
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
def get_equipment_dynamic_benchmarking_data(request, unit=None, console=None, equipment=None):
    """
    This function will get the values for the equipment level as well as console level dynamic benchmarking
    :param request: request django object
    :param unit: unit name
    :param console: console name will be provided
    :param equipment: equipment name will be provided
    :return: json response
    """
    query_params = obj = None
    try:
        if InputValidation.df[
            (InputValidation.df.unit_name == unit) & (InputValidation.df.console_name == console) & (
                    InputValidation.df.equipment_tag_name == equipment)].empty:
            return JsonResponse(
                {MESSAGE_KEY: "This {} or {} or {} is not registered with us !".format(unit, console,
                                                                                       equipment)}, safe=False,
                status=HTTP_404_NOT_FOUND)

    except Exception as e:
        log_error(e)
        return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(str(e))}, safe=False,
                            status=HTTP_500_INTERNAL_SERVER_ERROR)

    try:

        query_params = {
            TAG_NAME_REQUEST: request.GET[TAG_NAME_REQUEST],
            START_DATE_REQUEST: request.GET[START_DATE_REQUEST],
            END_DATE_REQUEST: request.GET[END_DATE_REQUEST]
        }

    except:
        pass

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = DynamicBenchmarking(query_params, unit, console, equipment)
                return obj.get_values()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)

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
