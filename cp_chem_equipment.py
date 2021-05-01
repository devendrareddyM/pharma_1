"""
File                :   overview_equipment.py

Description         :   This file will give all the feature value for the equipment level

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   13/8/19

Date Modified       :   6/12/2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
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


class EquipmentLevelFeatures(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the Equipment level features
    """

    def __init__(self, unit=None, console=None, equipment=None):
        """
        This will call the parent class to validate the connection
        :param unit: unit name will be provided
        :param console: console name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()
        self.console = console
        self.equipment = equipment
        self.unit = unit

    def composite_dict_object(self):

        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()

        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )

        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        dol = None
        tag_name = None
        tag_value = None
        timestamp = self.sync_time

        dynamic_benchmarking_status = 0
        equipment_health_status = GREEN_TAG
        instrument_drift_status = GREEN_TAG
        deviation_tracker_status = GREEN_TAG
        performance_tracker_status = GREEN_TAG

        dynamic_benchmarking_data = []
        external_targets_data = []
        performance_tags_data = []
        equipment_health_data = []
        filter_category_data = []
        instrument_drift_data = []
        deviation_tracker_data = []
        performance_tracker_data = []

        GRAPH_KEY = "graph"

        equipment_graph_data = []
        console_graph_data = []

        if self.console == HOT_CONSOLE_1_VALUE:
            dict_data = {
                TIMESTAMP_KEY: timestamp,
                "FDHDR_TAG": tag_name,
                "FDHDR_VALUE": tag_value,
                'DOL_VALUE': dol,

                DYNAMIC_BENCHMARKING: {
                    "status": dynamic_benchmarking_status,
                    "external_targets": external_targets_data,
                    "performance_tags": performance_tags_data,

                },

                EQUIPMENT_HEALTH: {
                    "status": equipment_health_status,
                    "data": equipment_health_data

                },

                INSTRUMENT_DRIFT: {
                    "status": instrument_drift_status,
                    "categories": filter_category_data,
                    "data": instrument_drift_data
                },

                DEVIATION_TRACKER: {
                    "status": deviation_tracker_status,
                    "stability_index": deviation_tracker_data,
                    "data": deviation_tracker_data

                },

                PERFORMANCE_TRACKER: {
                    "status": performance_tracker_status,
                    "data": performance_tracker_data

                },

                GRAPH_KEY: {"equipments": equipment_graph_data, "consoles": console_graph_data}
            }
            return dict_data
        else:
            dict_data = {
                TIMESTAMP_KEY: timestamp,

                DYNAMIC_BENCHMARKING: {
                    "status": dynamic_benchmarking_status,
                    "external_targets": external_targets_data,
                    "performance_tags": performance_tags_data,

                },

                EQUIPMENT_HEALTH: {
                    "status": equipment_health_status,
                    "data": equipment_health_data

                },

                INSTRUMENT_DRIFT: {
                    "status": instrument_drift_status,
                    "categories": filter_category_data,
                    "data": instrument_drift_data
                },

                DEVIATION_TRACKER: {
                    "status": deviation_tracker_status,
                    "stability_index": deviation_tracker_data,
                    "data": deviation_tracker_data

                },

                PERFORMANCE_TRACKER: {
                    "status": performance_tracker_status,
                    "data": performance_tracker_data

                },

                GRAPH_KEY: {"equipments": equipment_graph_data, "consoles": console_graph_data}
            }
            return dict_data

    def set_color_coding_tabular(self, dict_data):

        COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}'"
        try:
            self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment))

            tabular_df = pd.DataFrame(self._psql_session.fetchall())
            if not tabular_df.empty:
                dict_data["timestamp"] = str(tabular_df.timestamp.iloc[0])

                try:
                    dict_data["dynamic_benchmarking"]["status"] = int(
                        tabular_df[tabular_df["feature"] == DYNAMIC_BENCHMARKING][FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    pass

                try:
                    dict_data["equipment_health"]["status"] = int(
                        tabular_df[tabular_df["feature"] == EQUIPMENT_HEALTH][FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    pass

                try:
                    dict_data["instrument_drift"]["status"] = int(
                        tabular_df[tabular_df["feature"] == INSTRUMENT_DRIFT][FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    pass
                try:
                    dict_data["deviation_tracker"]["status"] = int(
                        tabular_df[tabular_df["feature"] == DEVIATION_TRACKER][FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    pass

                try:
                    dict_data["performance_tracker"]["status"] = \
                        int(tabular_df[tabular_df["feature"] == PERFORMANCE_TRACKER][FLAG_STATUS_VALUE].iloc[0])
                except Exception as e:
                    pass

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def set_color_coding_graph(self, dict_data):

        COLOR_CODING_GRAPH = "select * from color_coding_graph where equipment_name = '{}'"

        """
        Color Coding for graph data
        """

        try:
            self._psql_session.execute(COLOR_CODING_GRAPH.format(self.equipment))
            graph_df = pd.DataFrame(self._psql_session.fetchall())
            # TODO : Check this snippet
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

    def get_fdhdr_tag(self, dict_data):

        """
        this function is used to get the fdhdr value for the equipment and if fdhdr value of the equipment is not
        in 2,3,4,5 it makes the data is None
        """

        fdhr_value = None
        dol = None
        tag_name = None
        tag_value = None
        fdhdr = pd.DataFrame()

        """ 
        converting the current time into the epoch time stamp
        """
        utc_time = datetime.strptime(str(self.sync_time)[:-6], "%Y-%m-%d %H:%M:%S")
        epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
        epoch_time = int(epoch_time) * 1000

        """  
        checking the tag name is there for the particular equipment and assigning  the values 
        """

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

        """ 
        checking whether the data frame is empty or not
        """

        if not fdhdr.empty:
            tag_name = fdhdr["tag_name"].iloc[0]
            tag_value = int(fdhdr["tag_value"])

        if self.console == HOT_CONSOLE_1_VALUE:
            dict_data['FDHDR_TAG'] = tag_name
            dict_data['FDHDR_VALUE'] = tag_value
            dict_data["DOL_VALUE"] = dol

        """
        checking the FDHDR value is there in the range of 2,3,4,5 and make the data is none for the particular 
        equipment 
        """

        if fdhr_value not in CHECK_FDHDR_VALUE and self.console == HOT_CONSOLE_1_VALUE:
            dict_data["DOL_VALUE"] = None
            dict_data[INSTRUMENT_DRIFT]["categories"] = []
            dict_data[INSTRUMENT_DRIFT]["data"] = []
            dict_data[INSTRUMENT_DRIFT]["status"] = 0
            dict_data[DYNAMIC_BENCHMARKING]["status"] = 0
            dict_data[DYNAMIC_BENCHMARKING]["external_targets"] = []
            dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = []
            dict_data[EQUIPMENT_HEALTH]["status"] = 0
            dict_data[EQUIPMENT_HEALTH]["data"] = []
            dict_data[DEVIATION_TRACKER]["status"] = 0
            dict_data[DEVIATION_TRACKER]["data"] = []
            dict_data[DEVIATION_TRACKER]["stability_index"] = []
            dict_data[PERFORMANCE_TRACKER]["status"] = 0
            dict_data[PERFORMANCE_TRACKER]["data"] = []
            del dict_data['graph']

    def get_equipment_data_hotconsole1(self, dict_data):
        if self.console == HOT_CONSOLE_1_VALUE:
            try:
                self._psql_session.execute(CATEGORY_LIST_QUERY)
                category_df = pd.DataFrame(self._psql_session.fetchall())
                filter_category_data = yaml.safe_load(category_df.to_json(
                    orient="records"))
                dict_data[INSTRUMENT_DRIFT]["categories"] = filter_category_data
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            """
            Furnace Deviation Tracker
            """
            self._psql_session.execute(EQUIPMENT_STABILITY_INDEX.format(self.equipment, self.sync_time))
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data[DEVIATION_TRACKER]["stability_index"] = dict_data[DEVIATION_TRACKER][
                                                                  "stability_index"] + yaml.safe_load(
                df.to_json(orient=RECORDS))
            if df.empty:
                dict_data[DEVIATION_TRACKER]["status"] = 0

            try:
                self._psql_session.execute(DEATILED_DEVIATED_TAGS_DT.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[DEVIATION_TRACKER]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            """
            Execution of the query for the COT EFA ALGO which will initialize the df_cott from the cot_output table 
            from the postgres 
            in which we need to provide the furance name eg : (BA-101)  where this display will always be true
            ::display:: isa boolean column which helps to display the data on the compact view/equipment level
            """
            try:
                self._psql_session.execute(EQUIPMENT_COT.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            """
            Execution of the query for the TLE ALGO which will initialize the df_tle from the tle_value_table table from 
            the postgres 
            in which we need to provide the equipment_id eg : (BA-101)  where this compact_view_flag will always be true
            ::compact_view_flag:: is a boolean column which helps to display the data on the compact view/equipment level
            ::alert_flag:: is a color status of the tag
            """
            """THIS is for LBT for dummy data purpose doing"""
            if self.equipment in HOT_CONSOLE_1_EQUIPMENTS:
                try:
                    self._psql_session.execute(EQUIPMENT_LEVEL_EXTERNAL_TAGS2.format(self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data[DYNAMIC_BENCHMARKING]["external_targets"] = yaml.safe_load(df.to_json(orient=RECORDS))
                    dict_data[DYNAMIC_BENCHMARKING]["external_targets"] = yaml.safe_load(df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(
                        EQUIPMENT_LEVEL_PERFORMANCE_TAGS2.format(self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    else:
                        dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

                    dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            """Actual Query i need to add here over up.."""

            try:
                self._psql_session.execute(EQUIPMENT_TLE.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

                else:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                self._psql_session.execute(EQUIPMENT_FURNACE_O2.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

                else:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            """code for furnace  feed flow meters drift identification"""
            try:
                self._psql_session.execute(
                    EQUIPMENT_FURNACE_FEED_FLOW_DRIFT_QUERY.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

                else:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            """code for furnace dilution steam flow identification"""
            try:
                self._psql_session.execute(EQUIPMENT_FURNACE_DILUTION_STEAM_FLOW_QUERY.format(self.equipment,
                                                                                              self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))

                else:
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                d = pd.DataFrame()
                d = dict_data[INSTRUMENT_DRIFT]["data"]
                if d is None:
                    dict_data[INSTRUMENT_DRIFT]["status"] = 0

            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                self._psql_session.execute(EQUIPMENT_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment,
                                                                                              self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER][
                                                                 "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                if df.empty:
                    dict_data[PERFORMANCE_TRACKER]["status"] = 0
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                self._psql_session.execute(EQUIPMENT_FURNACE_RUN_LENGTH_QUERY.format(self.equipment,
                                                                                     self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    df = pd.DataFrame([{"equipment_name": self.equipment,
                                        "description": self.equipment + " Pred DOL",
                                        "actual_dol": df.actual_dol.iloc[0],
                                        "alert_flag": df.alert_flag.iloc[0],
                                        "predicted_coil_dol": df.predicted_dol.iloc[0]
                                        }])
                    dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH][
                                                              "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                if df.empty:
                    dict_data[EQUIPMENT_HEALTH]["status"] = 0

                return JsonResponse(dict_data, safe=False)
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
        elif self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE or self.console == COLD_CONSOLE_2_VALUE:
            if self.equipment in NON_FURNACE_EQUIPMENTS:
                try:
                    self._psql_session.execute(
                        EQUIPMENT_LEVEL_EXTERNAL_NON_FURNACES.format(self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data[DYNAMIC_BENCHMARKING]["external_targets"] = yaml.safe_load(
                            df.to_json(orient=RECORDS))
                    dict_data[DYNAMIC_BENCHMARKING]["external_targets"] = yaml.safe_load(df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(
                        EQUIPMENT_LEVEL_PERFORMANCE_NON_FURNACES.format(self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(
                            df.to_json(orient=RECORDS))

                    else:
                        dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(
                            df.to_json(orient=RECORDS))

                    dict_data[DYNAMIC_BENCHMARKING]["performance_tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(
                        DT_EQUIPMENT_COLD_CONSOLE2_POSTGRES_QUERY.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[DEVIATION_TRACKER]["stability_index"] = dict_data[DEVIATION_TRACKER][
                                                                          "stability_index"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                    if df.empty:
                        dict_data[DEVIATION_TRACKER]["status"] = 0
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                try:
                    self._psql_session.execute(DT_POSTGRES_QUERY.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        dict_data[DEVIATION_TRACKER]["data"] = dict_data[DEVIATION_TRACKER]["data"] + yaml.safe_load(
                            df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
            else:
                if DEBUG == ONE:
                    print("else condition executed")

    def get_equipment_data_coldconsole1(self, dict_data):
        if self.console == COLD_CONSOLE_1_VALUE:

            if self.equipment == "DA-301":

                try:
                    self._psql_session.execute(EQUIPMENT_DE_METHANIZER_DP.format(self.equipment,self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT][
                                                              "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                    if dict_data[INSTRUMENT_DRIFT]["data"]:
                        pass
                    else:
                        dict_data[INSTRUMENT_DRIFT]["status"] = 0
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            elif self.equipment == 'DA-203':

                self._psql_session.execute(
                    EQUIPMENT_EXCHANGER_HEALTH_MONITORING_PCA_QUERY.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())

                dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))

                if dict_data[EQUIPMENT_HEALTH]["data"]:
                    pass
                else:
                    dict_data[EQUIPMENT_HEALTH]["status"] = 0

            try:
                """
                     Surface Condenser data for the 'GB-201', 'GB-202' in the equipment health feature 
                    """
                self._psql_session.execute(
                    EQUIPMENT_EXCHANGER_HEALTH_MONITORING_PCA_QUERY.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
                if dict_data[EQUIPMENT_HEALTH]["data"]:
                    pass
                else:
                    dict_data[EQUIPMENT_HEALTH]["status"] = 0
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

            try:
                """
                    Surface condenser data for the 'GB-201', 'GB-202', 'GB-501', 'GB-601' in the instrument drift feature 
                    """
                self._psql_session.execute(EQUIPMENT_SURFACE_CONDENSER.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
                if dict_data[INSTRUMENT_DRIFT]["data"]:
                    pass
                else:
                    dict_data[INSTRUMENT_DRIFT]["status"] = 0
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            try:
                """
                Performance tracker data 
                """
                self._psql_session.execute(EQUIPMENT_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment,
                                                                                              self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
                if df.empty:
                    dict_data[PERFORMANCE_TRACKER]["status"] = 0
            except Exception as e:
                log_error("Exception due to : %s" + str(e))

    def get_equipment_data_coldconsole2(self, dict_data):
        if self.console == COLD_CONSOLE_2_VALUE:

            if self.equipment == "DA-401":

                try:
                    self._psql_session.execute(EQUIPMENT_DE_ETHANIZER_DP_QUERY.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

                try:
                    self._psql_session.execute(EQUIPMENT_DE_ETHANIZER_PPT_QUERY.format(self.sync_time, self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER][
                                                                 "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                    if df.empty:
                        dict_data[PERFORMANCE_TRACKER]["status"] = 0
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            elif self.equipment == "DA-404":
                """
                depropanizer_dp_result
                """
                try:
                    self._psql_session.execute(EQUIPMENT_DE_PROPANIZER_DP.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT][
                                                              "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            elif self.equipment == 'DA-405':

                try:
                    self._psql_session.execute(EQUIPMENT_DEBUTANIZER_DP.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[INSTRUMENT_DRIFT]["data"] = dict_data[INSTRUMENT_DRIFT][
                                                              "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                    if dict_data[INSTRUMENT_DRIFT]["status"] == 2:
                        dict_data[INSTRUMENT_DRIFT]["status"] = 1
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            elif self.equipment == "DA-403":

                try:
                    self._psql_session.execute(EQUIPMENT_C2_SPLITTER_EH.format(self.equipment))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH][
                                                              "data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

            elif self.equipment in ['DA-480', 'DA-490', 'DA-406']:
                """
                Exchanger Health for DA 480 and 490 and 406 
                """
                if self.equipment == "DA-406":

                    try:
                        self._psql_session.execute(
                            EQUIPMENT_EXCHANGER_HEALTH_406_EH.format(self.equipment, self.sync_time))
                        df = pd.DataFrame(self._psql_session.fetchall())
                        dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH][
                                                                  "data"] + yaml.safe_load(
                            df.to_json(orient=RECORDS))
                    except Exception as e:
                        log_error("Exception due to : %s" + str(e))
                else:
                    try:
                        self._psql_session.execute(
                            EQUIPMENT_EXCHANGER_HEALTH_480_490_EH.format(self.equipment, self.sync_time))
                        df = pd.DataFrame(self._psql_session.fetchall())
                        dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH][
                                                                  "data"] + yaml.safe_load(
                            df.to_json(orient=RECORDS))
                    except Exception as e:
                        log_error("Exception due to : %s" + str(e))
            if self.equipment not in ['DA-401', 'DA-404', 'DA-405']:
                dict_data[INSTRUMENT_DRIFT]["status"] = 0
            if self.equipment != "DA-401":
                try:
                    """
                    Performance tracker data 
                    """
                    self._psql_session.execute(EQUIPMENT_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment,
                                                                                                  self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER]["data"] + yaml.safe_load(
                        df.to_json(orient=RECORDS))
                    if df.empty:
                        dict_data[PERFORMANCE_TRACKER]["status"] = 0
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

        if dict_data[EQUIPMENT_HEALTH]["data"]:
            pass
        else:
            dict_data[EQUIPMENT_HEALTH]["status"] = 0

    def get_equipment_data_hotconsole2(self, dict_data):
        if self.console == HOT_CONSOLE_2_VALUE:
            """
               Surface Condenser data for the FA-155 in the equipment health feature 

               """
            if self.equipment != 'FA-155':
                dict_data[EQUIPMENT_HEALTH]["status"] = None
            dict_data[INSTRUMENT_DRIFT]["status"] = None
            if self.equipment == "FA-155":
                try:
                    self._psql_session.execute(
                        EQUIPMENT_EXCHANGER_HEALTH_MONITORING_PCA_QUERY.format(self.equipment, self.sync_time))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        if df['alert_flag'].iloc[0] == 2:
                            dict_data[EQUIPMENT_HEALTH]["status"] = GREEN_TAG

                        dict_data[EQUIPMENT_HEALTH]["data"] = dict_data[EQUIPMENT_HEALTH]["data"] + yaml.safe_load(
                            df.to_json(orient=RECORDS))

                except Exception as e:

                    log_error("Exception due to : %s" + str(e))

            try:
                """
                Performance tracker data 
                """
                self._psql_session.execute(EQUIPMENT_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment,
                                                                                              self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data[PERFORMANCE_TRACKER]["data"] = dict_data[PERFORMANCE_TRACKER]["data"] + yaml.safe_load(
                    df.to_json(orient=RECORDS))
                if df.empty:
                    dict_data[PERFORMANCE_TRACKER]["status"] = 0
            except Exception as e:

                log_error("Exception due to : %s" + str(e))

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
            dict_data = self.composite_dict_object()
            self.set_color_coding_tabular(dict_data)
            self.set_color_coding_graph(dict_data)
            self.get_equipment_data_hotconsole1(dict_data)
            self.get_equipment_data_coldconsole1(dict_data)
            self.get_equipment_data_coldconsole2(dict_data)
            self.get_equipment_data_hotconsole2(dict_data)
            self.get_fdhdr_tag(dict_data)

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
def get_equipment_level_data(request, unit_name=None, console_name=None, equipment_name=None):
    """
    This function will return the equipment level overview
    :param unit_name: unit name
    :param console_name: Console name
    :param equipment_name: equipment name
    :param request: request django object
    :return: json response
    """
    obj = None

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
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation.validate_token(request)
            if jwt_value:
                obj = EquipmentLevelFeatures(unit_name, console_name, equipment_name)
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
