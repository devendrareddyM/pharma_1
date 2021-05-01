"""
File                :   deviation_tracker.py

Description         :   This will return the deviation tracker value for the particular console
                        and equipment name

Author              :   LivNSense Technologies

Date Created        :   13-08-2019

Date Last modified :    13-08-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import time as t
from datetime import datetime

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database import InputValidation
from utilities.Constants import *
from utilities.http_request import error_instance
from utilities.LoggerFile import *
from Database.db_queries import *
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class DeviationTracker(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and respond for the deviation tracker value
    """

    def __init__(self, query_params, unit=None, console=None, equipment=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param unit: unit name will be provided
        :param console: console name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()

        self.console = console
        self.equipment = equipment
        self.unit = unit
        self.query_params = query_params

    def compose_dict_data_object(self):
        deviation_tracker_status = GREEN_TAG
        self.sync_time = None
        tag_name = []
        tag_value = []
        dol = []
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()
        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        if self.console == HOT_CONSOLE_1_VALUE:
            if not self.query_params:
                dict_data = {
                    "status": deviation_tracker_status,
                    TIMESTAMP_KEY: self.sync_time,
                    "FDHDR_TAG": tag_name,
                    "FDHDR_VALUE": tag_value,
                    "DOL_VALUE": dol,
                    STABILITY_INDEX_VALUE: None,
                    DATA_VALUE: []
                }
                return dict_data
            else:

                dict_data = []
                return dict_data
        else:

            if not self.query_params:
                dict_data = {
                    "status": deviation_tracker_status,
                    TIMESTAMP_KEY: self.sync_time,
                    STABILITY_INDEX_VALUE: None,
                    DATA_VALUE: []
                }
                return dict_data
            else:
                dict_data = []
                return dict_data

    def get_color_coding_tabular(self, dict_data):
        """ === """
        """
        Color Coding for tabular data
        """
        self.sync_time = None
        COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}' and feature = '{}' "

        # todo : respond back the empty json reponse
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()

        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )
        self.sync_time = timetsamp_record[TIMESTAMP_KEY]

        try:
            self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment, DEVIATION_TRACKER))

            tabular_df = pd.DataFrame(self._psql_session.fetchall())

            if not tabular_df.empty:

                try:

                    dict_data["status"] = int(
                        tabular_df[FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_fdhdr_tag(self, dict_data):

        """ this function is used to get the fdhdr value for the equipment and if fdhdr value of the equipment is not
                       in 2,3,4,5 we have to make the data is None"""
        fdhr_value = None
        dol = None
        tag_name = None
        tag_value = None
        fdhdr = pd.DataFrame()
        utc_time = datetime.strptime(str(self.sync_time)[:-6], "%Y-%m-%d %H:%M:%S")
        epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
        epoch_time = int(epoch_time) * 1000
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
                self._psql_session.execute(DOL_VALUE.format(i, epoch_time))
                data = self._psql_session.fetchall()
                dol_data = pd.DataFrame(data)
                if not dol_data.empty:
                    dol = dol_data['tag_value'].iloc[0]

        if not fdhdr.empty:
            tag_name = fdhdr["tag_name"].iloc[0]
            tag_value = int(fdhdr["tag_value"])

        if self.console == HOT_CONSOLE_1_VALUE:
            if not self.query_params:
                dict_data['FDHDR_TAG'] = tag_name
                dict_data['FDHDR_VALUE'] = tag_value
                dict_data["DOL_VALUE"] = dol

        if fdhr_value not in CHECK_FDHDR_VALUE and self.console == HOT_CONSOLE_1_VALUE:
            dict_data["status"] = 0
            dict_data["stability_index"] = 0
            dict_data["data"] = []

    def get_deviation_tracker_data(self, dict_data):
        deviation_tracker_df = pd.DataFrame()
        if self.console == HOT_CONSOLE_1_VALUE:
            if self.query_params:

                if self.query_params[START_DATE_REQUEST] == "":
                    self._psql_session.execute(
                        DETAILED_DEVIATED_TAGS_GRAPH_INCEPTION_ID.format(self.equipment,
                                                                         self.query_params[TAG_NAME_REQUEST],
                                                                         self.query_params[END_DATE_REQUEST]))

                else:
                    self._psql_session.execute(DETAILED_DEVIATED_TAGS_GRAPH_ID.format(
                        self.equipment,
                        self.query_params[TAG_NAME_REQUEST],
                        self.query_params[START_DATE_REQUEST],
                        self.query_params[END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"y_axis": list(df["tag_value"]),
                            "alert_flag": list(df["alert_flag"]), "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)

                return JsonResponse(dict_data, safe=False)
            else:
                try:
                    self._psql_session.execute(DEATILED_DEVIATED_TAGS_DT.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    deviation_tracker_df = deviation_tracker_df.append(df, ignore_index=True)
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(DEATILED_STABILITY_INDEX_DT.format(self.equipment, self.sync_time))
                    stability_value = self._psql_session.fetchone()

                    if stability_value:
                        dict_data[STABILITY_INDEX_VALUE] = stability_value[STABILITY_INDEX_VALUE]
                        dict_data[FLAG_STATUS_VALUE] = stability_value[FLAG_STATUS_VALUE]
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                dict_data[DATA_VALUE] = yaml.safe_load(deviation_tracker_df.to_json(orient=RECORDS))
                if dict_data[DATA_VALUE] is None:
                    dict_data["status"] = 0

            return JsonResponse(dict_data, safe=False)
        elif self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE or self.console == COLD_CONSOLE_2_VALUE:
            if not self.query_params:
                self._psql_session.execute(DT_POSTGRES_QUERY.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(
                    DT_EQUIPMENT_COLD_CONSOLE2_POSTGRES_QUERY.format(self.equipment, self.sync_time))
                stability_value = self._psql_session.fetchone()
                if stability_value:
                    dict_data[STABILITY_INDEX_VALUE] = stability_value[STABILITY_INDEX_VALUE]
                    dict_data[FLAG_STATUS_VALUE] = stability_value[FLAG_STATUS_VALUE]
                if df.shape[0]:
                    dict_data[DATA_VALUE] = yaml.safe_load(df.to_json(orient=RECORDS))
                if df.empty:
                    dict_data["status"] = 0
                else:
                    if DEBUG == ONE:
                        print("df is not empty")
            else:
                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAIL_DEVIATION_NON_FURNACE_INCEPTION.format(self.equipment,
                                                                      self.query_params[TAG_NAME_REQUEST],
                                                                      self.query_params[END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(DETAILED_NON_FURNACE_DEVIATED_TAGS_GRAPH_ID.format(
                        self.equipment,
                        self.query_params[TAG_NAME_REQUEST],
                        self.query_params[START_DATE_REQUEST],
                        self.query_params[END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"y_axis": list(df["tag_value"]),
                            "alert_flag": list(df["alert_flag"]), "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)
            return JsonResponse(dict_data, safe=False)

    def get_values(self):
        """
        This will return the data on the bases of the unit , equipment and console name for
        the deviation tracker values from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            dict_data = self.compose_dict_data_object()
            self.get_color_coding_tabular(dict_data)
            self.get_deviation_tracker_data(dict_data)
            self.get_fdhdr_tag(dict_data)

            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_equipment_deviation_tracker_data(request, unit=None, console=None, equipment=None):
    """
    This function will return the deviation tracker value
    :param unit: unit name
    :param console: Console name
    :param equipment: equipment name
    :param request: request django object
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
        log_error("Exception due to : %s" + str(e))
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
                obj = DeviationTracker(query_params, unit, console, equipment)
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
