"""
File                :   instrument_drift.py

Description         :   This will return the Instrument Drift value for the request parameters

Author              :   LivNSense Technologies

Date Created        :   26-08-2019

Date Last modified :    6-12-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import datetime

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime
from Database import InputValidation
from utilities.Constants import *
from utilities.http_request import error_instance
from utilities.LoggerFile import *
from Database.db_queries import *
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class InstrumentDrift(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and respond for the Instrument Drift
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

    def compose_dict_object(self):
        tag_name = []
        tag_value = []
        dol = []
        instrument_drift_status = 0
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()
        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )
        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        if self.console == HOT_CONSOLE_1_VALUE:
            if not self.query_params:
                dict_data = {
                    TIMESTAMP_KEY: self.sync_time,
                    "FDHDR_TAG": tag_name,
                    "FDHDR_VALUE": tag_value,
                    "DOL_VALUE": dol,
                    "status": instrument_drift_status,
                    "categories": [],
                    "data": []
                }
                return dict_data
            else:
                dict_data = []
                return dict_data
        else:
            if not self.query_params:
                dict_data = {
                    TIMESTAMP_KEY: self.sync_time,
                    "status": instrument_drift_status,
                    "categories": [],
                    "data": []
                }
                return dict_data
            else:
                dict_data = []
                return dict_data

    def get_color_coding_tabular(self, dict_data):
        # todo : respond back the empty json reponse
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()

        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )
        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}' and feature = '{}' "

        """
        Color Coding for tabular data
        """
        try:
            self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment, INSTRUMENT_DRIFT))

            tabular_df = pd.DataFrame(self._psql_session.fetchall())

            if not tabular_df.empty:
                if not self.query_params:
                    try:
                        dict_data["status"] = int(
                            tabular_df[FLAG_STATUS_VALUE].iloc[0])

                    except Exception as e:
                        log_error("Exception due to : %s" + str(e))
                else:
                    pass
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
                if self.console == HOT_CONSOLE_1_VALUE:
                    dict_data["status"] = 0
                    dict_data["categories"] = []
                    dict_data["data"] = []
            elif self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE or self.console == COLD_CONSOLE_2_VALUE :
                dict_data["DOL_VALUE"] = None

    def get_hotconsole1_id_data(self, dict_data):
        instrument_drift_df = pd.DataFrame()

        if self.console == HOT_CONSOLE_2_VALUE:
            dict_data["status"] = 0

        if self.console == HOT_CONSOLE_1_VALUE:
            try:
                self._psql_session.execute(CATEGORY_LIST_QUERY)
                category_df = pd.DataFrame(self._psql_session.fetchall())
                filter_category_data = yaml.safe_load(category_df.to_json(
                    orient="records"))
                if not self.query_params:
                    dict_data["categories"] = filter_category_data
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            # todo :  Temporary solution !!!

            if self.query_params:

                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_GRAPH_TLE_ID_NULL_START_DATE.format(self.equipment,
                                                                     self.query_params[TAG_NAME_REQUEST],
                                                                     self.query_params[END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_GRAPH_TLE_ID.format(self.equipment,
                                                     self.query_params[TAG_NAME_REQUEST],
                                                     self.query_params[START_DATE_REQUEST],
                                                     self.query_params[END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"actual": list(df["tag_value"]),
                            "predicted": list(df["predicted_value"]),
                            "alert_flag": list(df["alert_flag"]),
                            "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)

                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_GRAPH_FURNACE_O2_ID_NULL_START_DATE.format(self.equipment,
                                                                            self.query_params[TAG_NAME_REQUEST],
                                                                            self.query_params[END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_GRAPH_FURNACE_O2_ID.format(self.equipment,
                                                            self.query_params[TAG_NAME_REQUEST],
                                                            self.query_params[START_DATE_REQUEST],
                                                            self.query_params[END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"actual": list(df["tag_value"]),
                            "predicted": list(df["predicted_value"]),
                            "alert_flag": list(df["alert_flag"]),
                            "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)

                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_GRAPH_COT_ID_NULL_START_DATE.format(self.equipment,
                                                                     self.query_params[TAG_NAME_REQUEST],
                                                                     self.query_params[END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_GRAPH_COT_ID.format(self.equipment,
                                                     self.query_params[TAG_NAME_REQUEST],
                                                     self.query_params[START_DATE_REQUEST],
                                                     self.query_params[END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"actual": list(df["tag_value"]),
                            "predicted": list(df["predicted_value"]),
                            "alert_flag": list(df["alert_flag"]),
                            "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)

                """Furnace Feed Flow Meters Drift"""

                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_FURNACE_FEED_FLOW_METERS_DRIFT_GRAPH_ID_NULL_START_DATE.format(self.equipment,
                                                                                                self.query_params[
                                                                                                    TAG_NAME_REQUEST],
                                                                                                self.query_params[
                                                                                                    END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_FURNACE_FEED_FLOW_METERS_DRIFT_GRAPH_ID.format(self.equipment,
                                                                                self.query_params[TAG_NAME_REQUEST],
                                                                                self.query_params[
                                                                                    START_DATE_REQUEST],
                                                                                self.query_params[
                                                                                    END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"actual": list(df["tag_value"]),
                            "predicted": list(df["predicted_value"]),
                            "alert_flag": list(df["alert_flag"]),
                            "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)

                """Furnace Dilution Flow Meters Drift"""

                if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_FURNACE_DILUTION_STEAM_FLOW_GRAPH_ID_NULL_START_DATE.format(self.equipment,
                                                                                             self.query_params[
                                                                                                 TAG_NAME_REQUEST],
                                                                                             self.query_params[
                                                                                                 END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_FURNACE_DILUTION_STEAM_FLOW_GRAPH_ID.format(self.equipment,
                                                                             self.query_params[TAG_NAME_REQUEST],
                                                                             self.query_params[
                                                                                 START_DATE_REQUEST],
                                                                             self.query_params[
                                                                                 END_DATE_REQUEST]))

                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.sort_values("timestamp", ascending=True, inplace=True)
                    temp = {"actual": list(df["tag_value"]),
                            "predicted": list(df["predicted_value"]),
                            "alert_flag": list(df["alert_flag"]),
                            "x_axis": list(df["timestamp"]),
                            "unit": str(df["unit"].iloc[0])}
                    dict_data.append(temp)
                return JsonResponse(dict_data, safe=False)
            else:
                try:
                    self._psql_session.execute(DETAILED_TLE_ID.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        instrument_drift_df = instrument_drift_df.append(df)

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(DETAILED_FURNACE_O2_ID.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        instrument_drift_df = instrument_drift_df.append(df)

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(DETAILED_COT_ID.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        instrument_drift_df = instrument_drift_df.append(df)

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(DETAILED_FURNACE_FEED_FLOW_MATERS_DRIFT_ID.format(self.equipment,
                                                                                                 self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        instrument_drift_df = instrument_drift_df.append(df)

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(DETAILED_FURNACE_DILUTION_STEAM_FLOW_ID.format(self.equipment,
                                                                                              self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        instrument_drift_df = instrument_drift_df.append(df)

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    dict_data["data"] = yaml.safe_load(instrument_drift_df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                if instrument_drift_df is None:
                    dict_data["status"] = 0

            return JsonResponse(dict_data, safe=False)

    def get_coldconsole1_id_data(self, dict_data):

        instrument_drift_df = pd.DataFrame()
        if self.console == COLD_CONSOLE_1_VALUE:

            if self.equipment == 'DA-203':
                if not dict_data["data"]:
                    if dict_data["status"]:
                        dict_data["status"] = 0
            if self.equipment == 'GB-325':
                if not dict_data["data"]:
                    if dict_data["status"]:
                        dict_data["status"] = 0

            if self.equipment == 'DA-301':
                if self.query_params:
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(METHANIZER_DP_ID_GRAPH_START_DATE_NULL.format(
                            self.equipment, self.query_params["tag_name"],
                            self.query_params["end_date"]))
                    else:
                        self._psql_session.execute(METHANIZER_DP_ID_GRAPH_START_DATE.format(
                            self.equipment, self.query_params["tag_name"], self.query_params["start_date"],
                            self.query_params["end_date"]))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if df.shape[0]:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]), "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]), "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        dict_data.append(temp)
                    return JsonResponse(dict_data, safe=False)
                self._psql_session.execute(DETAILED_DE_METHANIZER_DP_ID.format(self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data["data"] = yaml.safe_load(df.to_json(orient=RECORDS))
                if not dict_data["data"]:
                    dict_data['status'] = 0
                else:
                    dict_data["status"] = 1

                return JsonResponse(dict_data, safe=False)

            """
            Surface Condenser data
            """
            if self.equipment in ['GB-201', 'GB-202', 'GB-501', 'GB-601']:
                if self.query_params:
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        print(DETAILED_SCP_GRAPH_ID_START_DATE_NULL.format(
                            self.equipment, self.query_params["tag_name"],
                            self.query_params["end_date"]))
                        self._psql_session.execute(DETAILED_SCP_GRAPH_ID_START_DATE_NULL.format(
                            self.equipment, self.query_params["tag_name"],
                            self.query_params["end_date"]))
                    else:
                        self._psql_session.execute(DETAILED_SCP_GRAPH_ID.format(
                            self.equipment, self.query_params["tag_name"], self.query_params["start_date"],
                            self.query_params["end_date"]))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if df.shape[0]:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]), "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]), "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        dict_data.append(temp)
                    return JsonResponse(dict_data, safe=False)

                self._psql_session.execute(DETAILED_SCP_ID.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data["data"] = yaml.safe_load(df.to_json(orient=RECORDS))
                if not dict_data["data"]:
                    dict_data['status'] = 0
                else:
                    dict_data["status"] = 1
                return JsonResponse(dict_data, safe=False)

            return JsonResponse([], safe=False)

    def get_coldconsole2_id_data(self, dict_data):
        instrument_drift_df = pd.DataFrame()
        if self.console == COLD_CONSOLE_2_VALUE:
            if self.equipment in ['DA-403', 'DA-406', 'DA-490', 'ARU', 'DA-480', 'DA-409']:
                if not dict_data["data"]:
                    if dict_data["status"]:
                        dict_data["status"] = 0
            if self.query_params:
                if self.equipment == 'DA-405':

                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_DEBUTANIZER_GRAPH_NULL_START_DATE.format(self.equipment,
                                                                              self.query_params[TAG_NAME_REQUEST],
                                                                              self.query_params[END_DATE_REQUEST]))
                    else:
                        self._psql_session.execute(
                            DETAILED_DEBUTANIZER_GRAPH.format(self.equipment,
                                                              self.query_params[TAG_NAME_REQUEST],
                                                              self.query_params[START_DATE_REQUEST],
                                                              self.query_params[END_DATE_REQUEST]))

                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]),
                                "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]),
                                "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        dict_data.append(temp)
                    return JsonResponse(dict_data, safe=False)

                if self.equipment == 'DA-401':
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_DE_ETHANIZER_DP_GRAPH_NULL_START_DATE_ID.format(self.equipment,
                                                                                     self.query_params[
                                                                                         TAG_NAME_REQUEST],
                                                                                     self.query_params[
                                                                                         END_DATE_REQUEST]))
                    else:
                        self._psql_session.execute(
                            DETAILED_DE_ETHANIZER_DP_GRAPH_ID.format(self.equipment,
                                                                     self.query_params[TAG_NAME_REQUEST],
                                                                     self.query_params[START_DATE_REQUEST],
                                                                     self.query_params[END_DATE_REQUEST]))

                    df = pd.DataFrame(self._psql_session.fetchall())

                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]),
                                "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]),
                                "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        dict_data.append(temp)

                    return JsonResponse(dict_data, safe=False)
                if self.equipment == 'DA-404':
                    if 'start_date' not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_DE_PROPANIZER_DP_GRAPH_NULL_START_DATE_ID.format(self.equipment,
                                                                                      self.query_params[
                                                                                          TAG_NAME_REQUEST],
                                                                                      self.query_params[
                                                                                          END_DATE_REQUEST]))
                    else:
                        self._psql_session.execute(
                            DETAILED_DE_PROPANIZER_DP_GRAPH_ID.format(self.equipment,
                                                                      self.query_params[TAG_NAME_REQUEST],
                                                                      self.query_params[START_DATE_REQUEST],
                                                                      self.query_params[END_DATE_REQUEST]))

                    df = pd.DataFrame(self._psql_session.fetchall())

                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"actual": list(df["tag_value"]),
                                "predicted": list(df["predicted_value"]),
                                "alert_flag": list(df["alert_flag"]),
                                "x_axis": list(df["timestamp"]),
                                "unit": str(df["unit"].iloc[0])}
                        dict_data.append(temp)
                    return JsonResponse(dict_data, safe=False)

            if self.equipment == 'DA-401':
                self._psql_session.execute(DETAILED_DE_ETHANIZER_DP_ID.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                instrument_drift_df = instrument_drift_df.append(df, ignore_index=True)

            """
            Instrument Drift for De-Propanizer
            """
            if self.equipment == 'DA-404':
                self._psql_session.execute(DETAILED_DE_PROPANIZER_DP_ID.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                instrument_drift_df = instrument_drift_df.append(df, ignore_index=True)

            """
            De-Butanizer
            """

            if self.equipment == 'DA-405':
                self._psql_session.execute(DETAILED_DE_BUTANIZER_DP_ID.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                instrument_drift_df = instrument_drift_df.append(df, ignore_index=True)
            dict_data["data"] = yaml.safe_load(instrument_drift_df.to_json(orient=RECORDS))
            if dict_data["data"]:
                if dict_data['status'] != 0 and dict_data['status'] != 2:
                    pass
                elif dict_data['status'] == 2:
                    dict_data['status'] = 1
                else:
                    dict_data['status'] = 1
            else:
                dict_data['status'] = 0
            return JsonResponse(dict_data, safe=False)

    def get_values(self):
        """
        This will return the data on the bases of the unit , equipment and console name for
        the deviation tracker values from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            dict_data = self.compose_dict_object()
            self.get_color_coding_tabular(dict_data)
            self.get_hotconsole1_id_data(dict_data)
            self.get_fdhdr_tag(dict_data)
            self.get_coldconsole1_id_data(dict_data)
            self.get_coldconsole2_id_data(dict_data)

            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
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
def get_instrument_drift_data(request, unit_name=None, console_name=None, equipment_name=None):
    """
    This function will return the Instrument drift features value
    :param unit_name: unit name
    :param console_name: Console name
    :param equipment_name: equipment name
    :param request: request django object
    :return: json response
    """
    query_params = obj = None

    try:

        if InputValidation.df[
            (InputValidation.df.unit_name == unit_name) & (InputValidation.df.console_name == console_name) & (
                    InputValidation.df.equipment_tag_name == equipment_name)].empty:
            return JsonResponse(
                {MESSAGE_KEY: "This {} or {} or {} is not registered with us !".format(unit_name, console_name,
                                                                                       equipment_name)}, safe=False,
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
                obj = InstrumentDrift(query_params, unit_name, console_name, equipment_name)
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
