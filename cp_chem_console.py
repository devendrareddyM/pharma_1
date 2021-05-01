"""
File                :   cp_chem_console.py

Description         :   This file will give all the feature value for the console level

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   13/8/19

Date Modified       :   6/12/19

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import datetime
import traceback
import time as t

import jwt
import pandas as pd
import yaml
import numpy as np
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from Database import InputValidation
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation

from Database.db_queries import *
from utilities.Constants import *
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR


class ConsoleLevelFeatures(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the Console level features
    """

    def __init__(self, unit=None, console=None, jwt_value=None, get_value=None):
        """
        This will call the parent class to validate the connection
        :param unit: unit name will be provided
        :param console: console name will be provided
        """
        super().__init__()
        self.console = console
        self.unit = unit
        self.jwt_value = jwt_value
        self.get_value = get_value

    def compose_dict_data_object(self):
        timestamp = (datetime.datetime.now()).strftime(
            UTC_DATE_TIME_FORMAT)

        dynamic_benchmarking_status = GREEN_TAG
        equipment_health_status = GREEN_TAG
        instrument_drift_status = GREEN_TAG
        deviation_tracker_status = GREEN_TAG
        performance_tracker_status = GREEN_TAG

        dynamic_benchmarking_data = []
        equipment_health_data = []
        instrument_drift_data = []
        deviation_tracker_data = []
        performance_tracker_data = []

        GRAPH_KEY = "graph"

        equipment_graph_data = []
        console_graph_data = []

        dict_data = {
            TIMESTAMP_KEY: timestamp,

            DYNAMIC_BENCHMARKING: {
                "status": dynamic_benchmarking_status,
                "data": dynamic_benchmarking_data

            },

            EQUIPMENT_HEALTH: {
                "status": equipment_health_status,
                "data": equipment_health_data

            },

            INSTRUMENT_DRIFT: {
                "status": instrument_drift_status,
                "data": instrument_drift_data
            },

            DEVIATION_TRACKER: {
                "status": deviation_tracker_status,
                "data": deviation_tracker_data

            },

            PERFORMANCE_TRACKER: {
                "status": performance_tracker_status,
                "data": performance_tracker_data

            },

            GRAPH_KEY: {"equipments": equipment_graph_data, "consoles": console_graph_data}
        }

        return dict_data

    def set_graphdata_colorcoding(self, COLOR_CODING_GRAPH, dict_data):

        try:
            if self.console == HOT_CONSOLE_1_VALUE:

                curr_time = t.time()
                curr_time = int(curr_time - (curr_time % 60) - 180) * 1000
                self._psql_session.execute(COLOR_CODING_GRAPH.format(self.console))
                graph_df = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(INFERENTIAL_COMMENT)
                comment = pd.DataFrame(self._psql_session.fetchall())
                if not comment.empty:
                    comment["comment"] = comment["equipment_name"].isin(HOT_CONSOLE_1_EQUIPMENTS)
                    comment_status = comment.drop_duplicates()
                    graph_df = pd.merge(graph_df, comment_status, on='equipment_name', how='left')

                for i in HOT_CONSOLE_1_EQUIPMENTS:

                    if i not in graph_df.values:

                        graph_df = graph_df.append(
                            {'timestamp': np.NaN, 'feature': None, 'console_name': 'Hot Console 1',
                             'equipment_name': i, 'console_flag': np.NaN, 'alert_flag': 0,
                             'concern': np.NaN}, ignore_index=True)

                    else:
                        pass

                self._psql_session.execute(FDHDR_TAG)

                data = self._psql_session.fetchall()
                d = pd.DataFrame(data)
                if d.empty:
                    d['tag_name'] = None
                    d['tag_value'] = None
                graph_df['split'] = graph_df['equipment_name'].str[-2:]
                d['split'] = d['tag_name'].str[-2:]
                graph_df = pd.merge(graph_df, d, on='split', how='left')
                graph_df.drop(graph_df.columns[[8]], axis=1, inplace=True)

                graph_df["FDHDR_TAG"] = graph_df["tag_name"]
                graph_df["FDHDR_VALUE"] = graph_df["tag_value"]
                graph_df.loc[graph_df['FDHDR_VALUE'] > BLOCK_OUT_STATE, 'alert_flag'] = 0

                if d.empty:
                    graph_df["FDHDR_VALUE"] = graph_df["FDHDR_VALUE"]
                else:
                    graph_df["FDHDR_VALUE"] = graph_df["FDHDR_VALUE"].astype(int)
                graph_df["alert_flag"] = graph_df["alert_flag"].astype(int)

                if comment.empty:
                    graph_df["comment"] = None
                # TODO : Check this snippet

                if not graph_df.empty:
                    equipment_graph_data = yaml.safe_load(
                        graph_df[["equipment_name", "alert_flag", "FDHDR_TAG", "FDHDR_VALUE", "comment"]].to_json(
                            orient=RECORDS))

                    console_graph_data = yaml.safe_load(
                        graph_df[["console_name", "console_flag"]].groupby(
                            'console_name').head(1).to_json(
                            orient=RECORDS))
                    dict_data["graph"]["equipments"] = equipment_graph_data
                    dict_data["graph"]["consoles"] = console_graph_data

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def set_tabulardata_colorcoding(self, COLOR_CODING_TABULAR, dict_data):
        try:
            if self.console == HOT_CONSOLE_1_VALUE:
                self._psql_session.execute(COLOR_CODING_TABULAR.format(self.console))
                tabular_df = pd.DataFrame(self._psql_session.fetchall())
                if not tabular_df.empty:
                    tabular_df.sort_values("equipment_name", ascending=True, inplace=True)
                    timestamp = str(tabular_df.timestamp.iloc[0])
                    dict_data["timestamp"] = timestamp
                    try:
                        dict_data["dynamic_benchmarking"]["status"] = int(
                            tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING]["console_flag"].iloc[0])

                    except Exception as e:
                        pass
                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        dict_data["dynamic_benchmarking"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]].to_json(orient=RECORDS))
                        if not dict_data["dynamic_benchmarking"]["data"]:
                            dict_data["dynamic_benchmarking"]["status"] = 0

                    except Exception as e:
                        pass
                    try:

                        dict_data["equipment_health"]["status"] = int(
                            tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][FLAG_STATUS_VALUE].iloc[0])
                    except Exception as e:
                        pass

                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]

                        dict_data["equipment_health"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME, "concern"]].to_json(orient=RECORDS))
                        if not dict_data["equipment_health"]["data"]:
                            dict_data["equipment_health"]["status"] = 0
                    except Exception as e:
                        pass

                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        dict_data["instrument_drift"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]].to_json(orient=RECORDS))
                    except Exception as e:
                        pass

                    try:
                        if not dict_data["instrument_drift"]["data"]:
                            dict_data["instrument_drift"]["status"] = 0
                        else:
                            dict_data["instrument_drift"]["status"] = int(
                                tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][FLAG_STATUS_VALUE].iloc[0])

                    except Exception as e:
                        pass

                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        dict_data["deviation_tracker"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]].to_json(orient=RECORDS))
                    except Exception as e:
                        pass

                    try:
                        if not dict_data["deviation_tracker"]["data"]:
                            dict_data["deviation_tracker"]["status"] = 0
                        else:
                            dict_data["deviation_tracker"]["status"] = int(
                                tabular_df[tabular_df["feature"] == DEVIATION_TRACKER]["console_flag"].iloc[0])
                    except Exception as e:
                        pass

                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        dict_data["performance_tracker"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]].head(1).to_json(orient=RECORDS))
                    except Exception as e:
                        pass
                        # log_error(e)

                    try:
                        if not dict_data["performance_tracker"]["data"]:
                            dict_data["performance_tracker"]["status"] = 0
                        else:
                            dict_data["performance_tracker"]["status"] = \
                                int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER]["console_flag"].iloc[0])
                    except Exception as e:
                        pass

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_datafor_deviationtracker(self, dict_data):
        # if self.console == HOT_CONSOLE_1_VALUE and self.get_value == 1:
        #     pass
        # else:
        #     try:
        #         self._psql_session.execute(DT_CONSOLE_COLD_CONSOLE2_POSTGRES_QUERY.format(self.console))
        #         df = pd.DataFrame(self._psql_session.fetchall())
        #         dict_data[DEVIATION_TRACKER]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
        #             df.to_json(orient=RECORDS))
        #     except Exception as e:
        #         log_error("Exception due to : %s" + str(e))
        pass

    def get_datafor_hotconse1(self, dict_data):
        if self.console == HOT_CONSOLE_1_VALUE and self.get_value == 1:
            """
            Initiazling the dataframe with the column name which will be used at the UI (dashboard) where the column
            name is been treated as a json key in the final json payload
            """
            """For checking the console level for LBT..For some non related query"""
            pass

    def get_datafor_hotconsole2(self, COLOR_CODING_GRAPH, COLOR_CODING_TABULAR, dict_data):
        if (self.console == HOT_CONSOLE_2_VALUE and self.get_value == 2) or (
                self.console == COLD_CONSOLE_1_VALUE and self.get_value == 3) or (
                self.console == COLD_CONSOLE_2_VALUE and self.get_value == 4):
            """
            This will query to get the console  level details for exchanger health
             based on the latest timestamp and convert it into dataframe and append the dataframe 
            to equipment_health_df dataframe.Send the final data frame as a json response to UI
            """
            # try:
            #     """
            #     Equipment Health
            #     """
            #     self._psql_session.execute(CONSOLE_EXCHANGER_HEALTH_PCA_QUERY)
            #     df = pd.DataFrame(self._psql_session.fetchall())
            #     dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
            #         df.to_json(orient=RECORDS))
            # except Exception as e:
            #     log_error("Exception due to : %s" + str(e))
            try:
                curr_time = t.time()
                curr_time = int(curr_time - (curr_time % 60) - 180) * 1000
                self._psql_session.execute(COLOR_CODING_GRAPH.format(self.console))
                graph_df = pd.DataFrame(self._psql_session.fetchall())
                if not graph_df.empty:
                    graph_df["alert_flag"] = graph_df["alert_flag"].astype(int)
                    # TODO : Check this snippet
                    graph_df["FDHDR_TAG"] = None
                    graph_df["FDHDR_VALUE"] = None
                    graph_df["comment"] = None

                if not graph_df.empty:
                    graph_df = graph_df.drop_duplicates(subset=['equipment_name'], keep='first')
                    equipment_graph_data = yaml.safe_load(
                        graph_df[["equipment_name", "alert_flag", "FDHDR_TAG", "FDHDR_VALUE", "comment"]].to_json(
                            orient=RECORDS))

                    console_graph_data = yaml.safe_load(
                        graph_df[["console_name", "console_flag"]].groupby(
                            'console_name').head(1).to_json(
                            orient=RECORDS))
                    dict_data["graph"]["equipments"] = equipment_graph_data
                    dict_data["graph"]["consoles"] = console_graph_data

            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            try:
                self._psql_session.execute(COLOR_CODING_TABULAR.format(self.console))
                tabular_df = pd.DataFrame(self._psql_session.fetchall())
                if tabular_df.empty:
                    dict_data["deviation_tracker"]["status"] = 0
                    dict_data["dynamic_benchmarking"]["status"] = 0
                    dict_data["instrument_drift"]["status"] = 0
                    dict_data["performance_tracker"]["status"] = 0
                    dict_data["equipment_health"]["status"] = 0
                if not tabular_df.empty:
                    tabular_df.sort_values("equipment_name", ascending=True, inplace=True)
                    timestamp = str(tabular_df.timestamp.iloc[0])
                    dict_data["timestamp"] = timestamp
                    try:
                        db_dt = tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]]
                        db_dt_data = db_dt.drop_duplicates(subset=['equipment_name'], keep='first')
                        db_dt_data.sort_values(by=['equipment_name'])
                        if db_dt_data[db_dt_data[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2:
                            dict_data["dynamic_benchmarking"]["status"] = RED_TAG
                        else:
                            dict_data["dynamic_benchmarking"]["status"] = int(
                                db_dt_data['alert_flag'].iloc[0])

                    except Exception as e:
                        pass
                    try:
                        # tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        db_dt = tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]]
                        db_dt_data = db_dt.drop_duplicates(subset=['equipment_name'], keep='first')
                        if not db_dt_data.empty:
                            db_dt_data.sort_values(by=['equipment_name'])
                            dict_data["dynamic_benchmarking"]["data"] = yaml.safe_load(
                                db_dt_data.to_json(orient=RECORDS))
                        if not dict_data["dynamic_benchmarking"]["data"]:
                            dict_data["dynamic_benchmarking"]["status"] = 0

                    except Exception as e:
                        pass

                    try:
                        # tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        db_h = tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME, "concern"]]
                        db_h_data = db_h.drop_duplicates(subset=['equipment_name'], keep='first')
                        if not db_h_data.empty:
                            db_h_data.sort_values(by=['equipment_name'])
                            dict_data["equipment_health"]["data"] = yaml.safe_load(db_h_data.to_json(orient=RECORDS))
                        if not dict_data["equipment_health"]["data"]:
                            dict_data["equipment_health"]["status"] = 0
                    except Exception as e:
                        pass
                    try:
                        if not dict_data["equipment_health"]["data"]:
                            dict_data["equipment_health"]["status"] = 0
                        else:
                            e_id = tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][FLAG_STATUS_VALUE]
                            if not e_id.empty:
                                if len(e_id) != 0 and e_id['alert_flag'].iloc[0] == 2:
                                    dict_data["equipment_health"]["status"] = GREEN_TAG
                                else:
                                    dict_data["equipment_health"]["status"] = int(
                                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][FLAG_STATUS_VALUE].iloc[
                                            0])
                    except Exception as e:
                        pass

                    try:
                        # tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        db_id = tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]]
                        db_id_data = db_id.drop_duplicates(subset=['equipment_name'], keep='first')
                        if not db_id_data.empty:
                            db_id_data.sort_values(by=['equipment_name'])
                            dict_data["instrument_drift"]["data"] = yaml.safe_load(db_id_data.to_json(orient=RECORDS))
                    except Exception as e:
                        pass

                    try:
                        if not dict_data["instrument_drift"]["data"]:
                            dict_data["instrument_drift"]["status"] = 0
                        else:
                            f_id = tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][FLAG_STATUS_VALUE]
                            if not f_id.empty:
                                if len(f_id) == 1 and f_id['alert_flag'].iloc[0] == 2:
                                    dict_data["instrument_drift"]["status"] = GREEN_TAG
                                if f_id['alert_flag'].iloc[0] == 2:
                                    dict_data["instrument_drift"]["status"] = GREEN_TAG
                                else:
                                    dict_data["instrument_drift"]["status"] = int(
                                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][FLAG_STATUS_VALUE].iloc[
                                            0])

                    except Exception as e:
                        pass

                    try:
                        # tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        db_dt = tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]]
                        db_dt_data = db_dt.drop_duplicates(subset=['equipment_name'], keep='first')
                        if not db_dt_data.empty:
                            db_dt_data.sort_values(by=['equipment_name'])
                            dict_data["deviation_tracker"]["data"] = yaml.safe_load(db_dt_data.to_json(orient=RECORDS))
                    except Exception as e:
                        pass

                    try:
                        if not dict_data["deviation_tracker"]["data"]:
                            dict_data["deviation_tracker"]["status"] = 0
                        else:
                            dict_data["deviation_tracker"]["status"] = int(
                                tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][FLAG_STATUS_VALUE].iloc[0])
                    except Exception as e:
                        pass

                    try:
                        tabular_df = tabular_df[tabular_df.alert_flag.isin(color_code_condition)]
                        dict_data["performance_tracker"]["data"] = yaml.safe_load(
                            tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                                ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]].to_json(orient=RECORDS))
                        db_pt = tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                            ["console_name", FLAG_STATUS_VALUE, EQUIPMENT_NAME]]
                        db_pt_data = db_pt.drop_duplicates(subset=['equipment_name'], keep='first')
                        if not db_pt_data.empty:
                            db_pt_data.sort_values(by=['equipment_name'])
                            dict_data["performance_tracker"]["data"] = yaml.safe_load(
                                db_pt_data.to_json(orient=RECORDS))
                    except Exception as e:
                        pass
                        # log_error(e)

                    try:
                        if not dict_data["performance_tracker"]["data"]:
                            dict_data["performance_tracker"]["status"] = 0
                        else:
                            dict_data["performance_tracker"]["status"] = \
                                int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][FLAG_STATUS_VALUE].iloc[0])
                    except Exception as e:
                        pass

            except Exception as e:
                log_error("Exception due to : %s" + str(e))

    def get_datafor_coldconsole1(self, dict_data):
        if self.console == COLD_CONSOLE_1_VALUE and self.get_value == 3:
            try:
                """
                Surface condenser data for the ['GB-201', 'GB-202', 'GB-501', 'GB-601'] in the instrument drift feature 
                """
                self._psql_session.execute(CONSOLE_SURFACE_CONDENSER_ID)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                self._psql_session.execute(CONSOLE_EXCHANGER_HEALTH_MONITORING_QUERY)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

    def get_datafor_coldconsole2(self, dict_data):
        if self.console == COLD_CONSOLE_2_VALUE and self.get_value == 4:
            try:
                """
                DeEthanizer dp
                """
                self._psql_session.execute(CONSOLE_DE_ETHANIZER_DP_ID)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                Demethanizer
                """
                self._psql_session.execute(CONSOLE_DE_METHANIZER_DP_ID)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                Depropanizer
                """
                self._psql_session.execute(CONSOLE_DE_PROPANIZER_DP_ID)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                Debutanizer
                """
                self._psql_session.execute(CONSOLE_DEBUTANIZER_DP_ID)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                Exchanger health
                """
                self._psql_session.execute(CONSOLE_EXCHANGER_HEALTH_EH)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                c2 splitter
                """
                self._psql_session.execute(CONSOLE_C2_SPLITTER_EH)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                De-Ethanizer 
                """
                self._psql_session.execute(CONSOLE_DE_ETHANIZER_PPT_QUERY)
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

    def get_values(self, date_time=None):
        """
        This will return the data on the bases of the unit  and console name for
        all the features from the Database .
        :return: Json Response
        """

        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            COLOR_CODING_GRAPH = "select * from color_coding_graph where console_name = '{}'"

            COLOR_CODING_TABULAR = "select * from color_coding_tabular where console_name = '{}'"
            """
            Color Coding for graph data
            """
            dict_data = self.compose_dict_data_object()
            self.set_graphdata_colorcoding(COLOR_CODING_GRAPH, dict_data)

            """
            Color Coding for tabular data
            """
            self.set_tabulardata_colorcoding(COLOR_CODING_TABULAR, dict_data)

            """
            1. This condition will be true when it will request is for not hot console 1
            2. When it is true this will give the data for the Deviation Tracker algo which is only aplicable on
                Hot Console 2  , Cold Console 1 , Cold Console 2
            """
            # self.get_datafor_deviationtracker(dict_data)

            """
            If the request is for the hot console 1 . it will get the data for COT Effulent Analyzer and TLE Algorithm
            """
            self.get_datafor_hotconse1(dict_data)
            self.get_datafor_hotconsole2(COLOR_CODING_GRAPH, COLOR_CODING_TABULAR, dict_data)
            # self.get_datafor_coldconsole1(dict_data)
            # self.get_datafor_coldconsole2(dict_data)
            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error('Exception due to : %s' + str(e))
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_console_level_data(request, unit_name=None, console_name=None):
    """
    This function will return the console level overview
    :param unit: unit name
    :param console: Console name
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if InputValidation.df[
            (InputValidation.df.unit_name == unit_name) & (InputValidation.df.console_name == console_name)].empty:
            return JsonResponse(
                {MESSAGE_KEY: "This {} or {} is not registered with us !".format(unit_name, console_name)}, safe=False,
                status=HTTP_404_NOT_FOUND)
    except Exception as e:
        log_error("Exception due to : %s" + str(e))
        return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(str(e))}, safe=False,
                            status=HTTP_500_INTERNAL_SERVER_ERROR)

    try:

        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)

            if console_name == HOT_CONSOLE_1_VALUE and (
                    jwt_value['role'] in ['superadmin', 'admin', 'engineer'] or jwt_value[
                'loggedin_useremail'] in ['operator1@cpchem.com']):
                get_value = 1
            elif console_name == HOT_CONSOLE_2_VALUE and (
                    jwt_value['role'] in ['superadmin', 'admin', 'engineer'] or jwt_value[
                'loggedin_useremail'] in ['operator2@cpchem.com']):
                get_value = 2
            elif console_name == COLD_CONSOLE_1_VALUE and (jwt_value['role'] in ['superadmin', 'admin', 'engineer'] or
                                                           jwt_value[
                                                               'loggedin_useremail'] in ['operator3@cpchem.com']):
                get_value = 3
            elif console_name == COLD_CONSOLE_2_VALUE and (jwt_value['role'] in ['superadmin', 'admin', 'engineer'] or
                                                           jwt_value[
                                                               'loggedin_useremail'] in ['operator4@cpchem.com']):
                get_value = 4
            else:
                return JsonResponse({MESSAGE_KEY: ROLE_PERMISSION}, status=HTTP_403_FORBIDDEN)
            if jwt_value:
                obj = ConsoleLevelFeatures(unit_name, console_name, jwt_value, get_value)
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
