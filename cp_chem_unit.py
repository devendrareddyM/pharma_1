"""
File                :   cp_chem_unit.py

Description         :   This file will give all the feature value for the unit(plant) level

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   13/8/19

Date Modified       :   6/12/19

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import datetime
import time as t

import pandas as pd
import yaml
from django.views.decorators.csrf import csrf_exempt

from Database import InputValidation
from utilities.Constants import *
from utilities.http_request import *
from utilities.LoggerFile import *
from Database.db_queries import *
from utilities.api_response import *
from Database.Authentiction_tokenization import _PostGreSqlConnection, _TokenValidation


class UnitLevelFeatures(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the Unit level features
    """

    def __init__(self, unit=None, jwt_value=None):
        """
        This will call the parent class to validate the connection
        :param unit: unit name will be provided
        :param console: console name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()
        self.unit = unit
        self.jwt_value = jwt_value

    def compose_dict_data_object(self):
        timestamp = (datetime.datetime.now()).strftime(
            UTC_DATE_TIME_FORMAT)

        dynamic_benchmarking_status = None
        equipment_health_status = None
        instrument_drift_status = None
        deviation_tracker_status = None
        performance_tracker_status = None

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

    def set_color_coding_graph(self, COLOR_CODING_GRAPH, dict_data):
        try:
            curr_time = t.time()
            curr_time = int(curr_time - (curr_time % 60) - 120) * 1000
            self._psql_session.execute(COLOR_CODING_GRAPH)
            graph_df = pd.DataFrame(self._psql_session.fetchall())
            self._psql_session.execute(
                FDHDR_TAG)
            data = self._psql_session.fetchall()
            d = pd.DataFrame(data)
            if d.empty:
                d['tag_name'] = None
                d['tag_value'] = None
            graph_df['split'] = graph_df['equipment_name'].str[-2:]
            d['split'] = d['tag_name'].str[-2:]
            graph_df = pd.merge(graph_df, d, on='split', how='left')
            graph_df.drop(graph_df.columns[[7]], axis=1, inplace=True)
            graph_df.columns = [' timestamp ', 'feature', 'console_name', 'equipment_name', 'console_flag',
                                'alert_flag', 'concern', 'FDHDR_TAG', 'FDHDR_VALUE']

            graph_df.loc[graph_df['FDHDR_VALUE'] > BLOCK_OUT_STATE, 'alert_flag'] = 0
            if d.empty:
                graph_df["FDHDR_VALUE"] = graph_df["FDHDR_VALUE"]
            else:
                graph_df["FDHDR_VALUE"] = graph_df["FDHDR_VALUE"]

            if not graph_df.empty:
                equipment_graph_data = yaml.safe_load(
                    graph_df[["equipment_name", "alert_flag"]].to_json(orient=RECORDS))
                console_graph_data = yaml.safe_load(
                    graph_df[["console_name", "console_flag"]].groupby(
                        'console_name').head(1).to_json(
                        orient=RECORDS))
                dict_data["graph"]["equipments"] = equipment_graph_data
                dict_data["graph"]["consoles"] = console_graph_data
        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def set_color_coding_tabular(self, COLOR_CODING_TABULAR, dict_data):
        try:
            self._psql_session.execute(COLOR_CODING_TABULAR)
            tabular_df = pd.DataFrame(self._psql_session.fetchall())
            if not tabular_df.empty:
                timestamp = str(tabular_df.timestamp.iloc[0])
                dict_data["timestamp"] = timestamp
                try:
                    dict_data["dynamic_benchmarking"]["data"] = yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", "console_flag"]].head(1).rename(
                            columns={"console_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))

                except Exception as e:
                    pass

                try:
                    dict_data["dynamic_benchmarking"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING]["console_flag"].iloc[0])

                except Exception as e:
                    pass

                try:
                    dict_data["equipment_health"]["data"] = yaml.safe_load(
                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                            ["console_name", "console_flag"]].head(1).rename(
                            columns={"console_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))

                except Exception as e:
                    pass

                try:
                    dict_data["equipment_health"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH]["console_flag"].iloc[0])

                except Exception as e:
                    pass

                try:
                    dict_data["instrument_drift"]["data"] = yaml.safe_load(
                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                            ["console_name", "console_flag"]].head(1).rename(
                            columns={"console_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                except Exception as e:
                    pass

                try:
                    dict_data["instrument_drift"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT]["console_flag"].iloc[0])
                except Exception as e:
                    pass

                try:
                    dict_data["deviation_tracker"]["data"] = yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                            ["console_name", "console_flag"]].head(1).rename(
                            columns={"console_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                except Exception as e:
                    pass

                try:
                    dict_data["deviation_tracker"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == DEVIATION_TRACKER]["console_flag"].iloc[0])
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["data"] = yaml.safe_load(
                        tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                            ["console_name", "console_flag"]].head(1).rename(
                            columns={"console_flag": FLAG_STATUS_VALUE})
                            .to_json(orient=RECORDS))
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER]["console_flag"].iloc[0])
                except Exception as e:
                    pass
        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_performance_tracker_data(self, dict_data):
        """
                   This will query to get the unit level details for deethanizer ppt based on the latest timestamp and convert it into dataframe and append the dataframe
                    to plant_performance_df dataframe. Send the final data frame as a json response to UI
                    """

        try:
            self._psql_session.execute(UNIT_DE_ETHANIZER_PPT_QUERY)
            df = pd.DataFrame(self._psql_session.fetchall())

            dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_equipment_health_data(self, dict_data):
        """
                    This will query to get the unit level details for exchanger health based on the latest timestamp and convert it into dataframe and append the dataframe
                    to equipment_health_df dataframe. Send the final data frame as a json response to UI
                    """

        """
        This will query to get the unit level details for c2 splitter  based on the latest timestamp and convert it into dataframe and append the dataframe 
        to equipment_health_df  dataframe. Send the final data frame as a json response to UI
        """

        try:
            """
            c2 splitter
            """
            self._psql_session.execute(UNIT_C2_SPLITTER_EH)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        """
        This will query to get the unit level details for exchanger health pca based on the latest timestamp and convert it into dataframe and append the dataframe 
        to equipment_health_df dataframe. Send the final data frame as a json response to UI
        """

        try:
            """
            Equipment Health for Hot Console 2
            """
            self._psql_session.execute(UNIT_EXCHANGER_HEALTH_PCA_QUERY)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_instrument_drift_data(self, dict_data):
        """
        This will query to get the unit level details for tle based on the latest timestamp and convert it into dataframe and append the dataframe
        to instrument_drift_df dataframe. Send the final data frame as a json response to UI
        """

        """
        This will query to get the unit level details for de_ethanizer_dp  based on the latest timestamp and convert it into dataframe and append the dataframe 
        to instrument_drift_df dataframe. Send the final data frame as a json response to UI
        """

        try:
            self._psql_session.execute(UNIT_DE_ETHANIZER_DP_QUERY)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))
        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        """
        This will query to get the unit level details for de_methanizer_dp  based on the latest timestamp and convert it into dataframe and append the dataframe 
        to instrument_drift_df dataframe. Send the final data frame as a json response to UI
        """

        try:
            self._psql_session.execute(UNIT_DE_METHANIZER_DP)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        """
        This will query to get the unit level details for de_propanizer_dp  based on the latest timestamp and convert it into dataframe and append the dataframe 
        to instrument_drift_df dataframe. Send the final data frame as a json response to UI
        """

        try:
            self._psql_session.execute(UNIT_DE_PROPANIZER_DP)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))
        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        """
        This will query to get the unit level details for debutanizer_dp  based on the latest timestamp and convert it into dataframe and append the dataframe 
        to instrument_drift_df dataframe. Send the final data frame as a json response to UI
        """

        try:
            self._psql_session.execute(UNIT_DEBUTANIZER_DP)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        try:
            """
            Surface condenser data for the ['GB-201', 'GB-202', 'GB-501', 'GB-601'] in the instrument drift feature 
            """
            self._psql_session.execute(UNIT_SURFACE_CONDENSER)
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                df.to_json(orient=RECORDS))
        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def get_hot_console2_unit_level_data(self, dict_data):
        if self.jwt_value['loggedin_userid'] not in self.jwt_value['permission']:
            COLOR_CODING_GRAPH = "select distinct(equipment_name),alert_flag,console_flag,console_name from " \
                                 "color_coding_graph " \
                                 "where " \
                                 "console_name !='Hot Console 1' "
            self._psql_session.execute(COLOR_CODING_GRAPH)
            graph_df = pd.DataFrame(self._psql_session.fetchall())
            COLOR_CODING_TABULAR = "select * from color_coding_tabular where console_name='Hot Console 2' and " \
                                   "alert_flag>1 "
            self._psql_session.execute(COLOR_CODING_TABULAR)
            tabular_df = pd.DataFrame(self._psql_session.fetchall())
            if not graph_df.empty:
                equipment_graph_data = yaml.safe_load(
                    graph_df[["equipment_name", "alert_flag"]].to_json(orient=RECORDS))
                console_graph_data = yaml.safe_load(
                    graph_df[["console_name", "console_flag"]].groupby(
                        'console_name').head(1).to_json(
                        orient=RECORDS))
                dict_data["graph"]["equipments"] = dict_data["graph"]["equipments"] + equipment_graph_data
                dict_data["graph"]["consoles"] = dict_data["graph"]["consoles"] + console_graph_data

            if not tabular_df.empty:
                try:
                    dict_data["dynamic_benchmarking"]["data"] = dict_data["dynamic_benchmarking"][
                                                                    "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["dynamic_benchmarking"]["data"]:
                        if dict_data["dynamic_benchmarking"][STATUS_KEY] is None:
                            try:
                                dict_data["dynamic_benchmarking"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["equipment_health"]["data"] = dict_data["equipment_health"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["equipment_health"]["data"]:
                        if dict_data[EQUIPMENT_HEALTH][STATUS_KEY] is None:
                            try:
                                dict_data["equipment_health"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["instrument_drift"]["data"] = dict_data["instrument_drift"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["instrument_drift"]["data"]:
                        if dict_data[INSTRUMENT_DRIFT][STATUS_KEY] is None:
                            try:
                                dict_data["instrument_drift"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["deviation_tracker"]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["deviation_tracker"]["data"]:
                        if dict_data[DEVIATION_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["deviation_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DEVIATION_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["data"] = dict_data["performance_tracker"][
                                                                   "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE})
                            .to_json(orient=RECORDS))
                    if dict_data["performance_tracker"]["data"]:
                        if dict_data[PERFORMANCE_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["performance_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

    def get_cold_console1_unit_level_data(self, dict_data):
        if self.jwt_value['loggedin_userid'] not in self.jwt_value['permission']:
            COLOR_CODING_TABULAR = "select * from color_coding_tabular where console_name='Cold Console 1' and " \
                                   "alert_flag>1 "
            self._psql_session.execute(COLOR_CODING_TABULAR)
            tabular_df = pd.DataFrame(self._psql_session.fetchall())
            if not tabular_df.empty:
                try:
                    dict_data["dynamic_benchmarking"]["data"] = dict_data["dynamic_benchmarking"][
                                                                    "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["dynamic_benchmarking"]["data"]:
                        if dict_data["dynamic_benchmarking"][STATUS_KEY] is None:
                            try:
                                dict_data["dynamic_benchmarking"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["equipment_health"]["data"] = dict_data["equipment_health"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["equipment_health"]["data"]:
                        if dict_data[EQUIPMENT_HEALTH][STATUS_KEY] is None:
                            try:
                                dict_data["equipment_health"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["instrument_drift"]["data"] = dict_data["instrument_drift"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["instrument_drift"]["data"]:
                        if dict_data[INSTRUMENT_DRIFT][STATUS_KEY] is None:
                            try:
                                dict_data["instrument_drift"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["deviation_tracker"]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["deviation_tracker"]["data"]:
                        if dict_data[DEVIATION_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["deviation_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DEVIATION_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["data"] = dict_data["performance_tracker"][
                                                                   "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE})
                            .to_json(orient=RECORDS))
                    if dict_data["performance_tracker"]["data"]:
                        if dict_data[PERFORMANCE_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["performance_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

    def get_cold_console2_unit_level_data(self, dict_data):
        if self.jwt_value['loggedin_userid'] not in self.jwt_value['permission']:
            COLOR_CODING_TABULAR = "select * from color_coding_tabular where console_name='Cold Console 2' and " \
                                   "alert_flag>1 "
            self._psql_session.execute(COLOR_CODING_TABULAR)
            tabular_df = pd.DataFrame(self._psql_session.fetchall())
            if not tabular_df.empty:
                try:
                    dict_data["dynamic_benchmarking"]["data"] = dict_data["dynamic_benchmarking"][
                                                                    "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["dynamic_benchmarking"]["data"]:
                        if dict_data["dynamic_benchmarking"][STATUS_KEY] is None:
                            try:
                                dict_data["dynamic_benchmarking"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["equipment_health"]["data"] = dict_data["equipment_health"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["equipment_health"]["data"]:
                        if dict_data[EQUIPMENT_HEALTH][STATUS_KEY] is None:
                            try:
                                dict_data["equipment_health"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass

                except Exception as e:
                    pass
                try:
                    dict_data["instrument_drift"]["data"] = dict_data["instrument_drift"]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["instrument_drift"]["data"]:
                        if dict_data[INSTRUMENT_DRIFT][STATUS_KEY] is None:
                            try:
                                dict_data["instrument_drift"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["deviation_tracker"]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE}).to_json(orient=RECORDS))
                    if dict_data["deviation_tracker"]["data"]:
                        if dict_data[DEVIATION_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["deviation_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == DEVIATION_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["data"] = dict_data["performance_tracker"][
                                                                   "data"] + yaml.safe_load(
                        tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][
                            ["console_name", "alert_flag"]].head(1).rename(
                            columns={"alert_flag": FLAG_STATUS_VALUE})
                            .to_json(orient=RECORDS))
                    if dict_data["performance_tracker"]["data"]:
                        if dict_data[PERFORMANCE_TRACKER][STATUS_KEY] is None:
                            try:
                                dict_data["performance_tracker"]["status"] = \
                                    int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER]["alert_flag"].iloc[0])
                            except Exception as e:
                                pass
                except Exception as e:
                    pass

    def get_deviation_tracker_data(self, dict_data):
        """
                   Deviation tracker for Hot Console 2 , Cold Console 1 , Cold Console 2
                   """

        try:
            if self.jwt_value['loggedin_userid'] not in self.jwt_value['permission']:
                self._psql_session.execute(DT_UNIT_COLD_CONSOLE2_POSTGRES_QUERY)
                df = pd.DataFrame(self._psql_session.fetchall())

                dict_data[DEVIATION_TRACKER]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

        """
        Furnace Deviation tracker for Hot Console 1
        """

    def get_values(self):
        """
        This will return the data on the bases of the unit , equipment and console name for
        all the features from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            COLOR_CODING_GRAPH = "select * from color_coding_graph where console_name='Hot Console 1'"
            COLOR_CODING_TABULAR = "select * from color_coding_tabular where console_name='Hot Console 1' and " \
                                   "console_flag>1 "
            dict_data = self.compose_dict_data_object()
            self.set_color_coding_graph(COLOR_CODING_GRAPH, dict_data)
            self.set_color_coding_tabular(COLOR_CODING_TABULAR, dict_data)
            self.get_hot_console2_unit_level_data(dict_data)
            self.get_cold_console1_unit_level_data(dict_data)
            self.get_cold_console2_unit_level_data(dict_data)

            # self.get_deviation_tracker_data(dict_data)
            # self.get_instrument_drift_data(dict_data)
            # self.get_equipment_health_data(dict_data)
            # self.get_performance_tracker_data(dict_data)

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
def get_unit_equipments_details(request, unit_name=None):
    """
    This function will return the unit level overview
    :param unit_name: unit name
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if InputValidation.df[InputValidation.df.unit_name == unit_name].empty:
            return JsonResponse({MESSAGE_KEY: "This {} is not registered with us !".format(unit_name)}, safe=False,
                                status=HTTP_404_NOT_FOUND)
    except Exception as e:
        log_error("Exception due to : %s" + str(e))
        return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(str(e))}, safe=False,
                            status=HTTP_500_INTERNAL_SERVER_ERROR)

    try:
        if request.method == GET_REQUEST:

            jwt_value = _TokenValidation().validate_token(request)

            if jwt_value['role'] not in ['superadmin', 'admin', 'engineer']:
                return JsonResponse({MESSAGE_KEY: ROLE_PERMISSION}, status=HTTP_403_FORBIDDEN)
            if jwt_value:
                obj = UnitLevelFeatures(unit_name, jwt_value)
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
