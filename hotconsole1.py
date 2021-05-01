"""
File                :   hot console1.py

Description         :   This will handle the color code philosophy for unit ,
                        console , equipment and feature level

Author              :   LivNSense Technologies

Date Created        :   20-11-2019

Date Last modified :    20-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from Database.Configuration import DATABASE_CONFIG
from utilities.Constants import *
from utilities.LoggerFile import log_error
from Database.db_queries import *
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR
from Database.Authentiction_tokenization import _GetSyncTime


class ColorCodingUtility:

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        """
        self.sync_time = _GetSyncTime().get_sync_timestamp()
        try:

            self.connection = psycopg2.connect(user=DATABASE_CONFIG[DEFAULT][USER],
                                               password=DATABASE_CONFIG[DEFAULT][PASSWORD],
                                               host=DATABASE_CONFIG[DEFAULT][HOST],
                                               port=DATABASE_CONFIG[DEFAULT][PORT],
                                               database=DATABASE_CONFIG[DEFAULT][NAME])
            self._psql_session = self.connection.cursor(cursor_factory=RealDictCursor)
            self._db_connection = True

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            self._db_connection = False

    @classmethod
    def get_empty_dataframe(cls):

        return pd.DataFrame(columns=[TIMESTAMP_KEY,
                                     FEATURE_COL,
                                     CONSOLE_NAME_VALUE,
                                     EQUIPMENT_NAME,
                                     CONSOLE_FLAG,
                                     FLAG_STATUS_VALUE,
                                     CONCERN_KEY
                                     ]).copy()

    @classmethod
    def get_color_count(cls, df):
        """
        This function will get the count for the color tags and status for the offline equipment
        :param df: inoput data-frame
        :return: status [boolean] and count for alert flags [GREEN ,  BLUE , YELLOW , RED] and data-frame
        """

        return df.shape[0] == df[df[FLAG_STATUS_VALUE] == OFFLINE_TAG].shape[0], \
               df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0], \
               df[df[FLAG_STATUS_VALUE] == BLUE_TAG].shape[0], \
               df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0], \
               df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0], \
               df

    def get_dataframe(self, queries_list):
        """
        This function will query from the Database for the provided query and will make dataframe and return
        :param queries_list: list of queries
        :return: dataframe
        """
        temp_df = pd.DataFrame()
        for query in queries_list:
            try:
                self._psql_session.execute(query)
                temp_df = temp_df.append(pd.DataFrame(self._psql_session.fetchall()), ignore_index=True)
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
        return temp_df

    @classmethod
    def equipment_level_DB(cls, offline_satatus, g_count, b_count, y_count, r_count, df):
        """
        Dynamic bench marking color coding function
        This will have the logic for the equipment level color coding
        :param offline_satatus: booleans status for the offline equipment
        :param g_count: green tag count
        :param b_count: blue tag count
        :param y_count: yellow tag count
        :param r_count: red tag count
        :param df: input data frame
        :return: time stamp , console_name , equipment_name , status
        """
        status = None
        external = np.logical_and((df[TYPE] == EXTERNALTAGS), (df[FLAG_STATUS_VALUE] == RED_TAG))
        perf_red = np.logical_and((df[TYPE] == PERFTAGS), (df[FLAG_STATUS_VALUE] == RED_TAG))
        perf_green = np.logical_and((df[TYPE] == PERFTAGS), (df[FLAG_STATUS_VALUE] == GREEN_TAG))
        perf_yellow = np.logical_and((df[TYPE] == PERFTAGS), (df[FLAG_STATUS_VALUE] == YELLOW_TAG))

        if external.any():
            status = RED_TAG
        elif not external.any():
            if perf_red.any():
                status = RED_TAG
            elif perf_green.any():
                status = GREEN_TAG
            elif perf_yellow.any():
                status = YELLOW_TAG
        else:
            if perf_red.any():
                status = RED_TAG
            elif perf_green.any():
                status = GREEN_TAG
            elif perf_yellow.any():
                status = YELLOW_TAG

        return {TIMESTAMP_KEY: df.timestamp.iloc[0],
                CONSOLE_NAME_VALUE: df.console_name.iloc[0],
                EQUIPMENT_NAME: df.equipment_name.iloc[0],
                FLAG_STATUS_VALUE: status,
                CONSOLE_FLAG: None,
                FEATURE_COL: DYNAMIC_BENCHMARKING,
                }


class HotConsole1(ColorCodingUtility):
    """
    This class is responsible to get the data  and construct the in memory object for color coding philospy
    for hot console 1
    """

    def __init__(self):

        super().__init__()
        self.HC1_EQUIPMENT_ID_DF = self.get_empty_dataframe()
        self.HC1_EQUIPMENT_DT_DF = self.get_empty_dataframe()
        self.HC1_EQUIPMENT_EH_DF = self.get_empty_dataframe()
        self.HC1_EQUIPMENT_PT_DF = self.get_empty_dataframe()
        self.HC1_EQUIPMENT_DB_DF = self.get_empty_dataframe()
        self.HC1_EQUIPMENT_ET_DF = self.get_empty_dataframe()

    @classmethod
    def equipment_level_instrument_drift(cls, offline_satatus, g_count, b_count, y_count, r_count, df):
        """
        This will have the logic for  color coding at the equipment level
        :param offline_satatus: booleans status for the offline equipment
        :param g_count: green tag count
        :param b_count: blue tag count
        :param y_count: yellow tag count
        :param r_count: red tag count
        :param df: input data frame
        :return: time stamp , console_name , equipment_name , status
        """

        if r_count >= 6 or y_count >= 13:
            status = RED_TAG
        elif 5 >= r_count >= 3 or 12 >= y_count >= 6 or (2 >= r_count >= 1 and 5 == y_count):
            status = YELLOW_TAG
        else:
            status = GREEN_TAG
        return {TIMESTAMP_KEY: df.timestamp.iloc[0],
                CONSOLE_NAME_VALUE: df.console_name.iloc[0],
                EQUIPMENT_NAME: df.equipment_name.iloc[0],
                FLAG_STATUS_VALUE: status,
                CONSOLE_FLAG: None,
                FEATURE_COL: INSTRUMENT_DRIFT,
                }

    def get_data_for_hc1_id_el(self):
        """
        This will return the data and construct the data frame object
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [COLOR_CODING_TLE.format(self.sync_time),
                            COLOR_CODING_FURNACE_O2.format(self.sync_time),
                            COLOR_CODING_COT.format(self.sync_time),
                            COLOR_CODING_FURNACE_FEED_FLOW_METER.format(self.sync_time),
                            COLOR_CODING_FURNACE_DILUTION_STEAM_FLOW.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)

            if not temp_df.empty:
                for furnace_name in temp_df.equipment_name.unique():
                    offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(
                        temp_df[temp_df.equipment_name == furnace_name])

                    self.HC1_EQUIPMENT_ID_DF = self.HC1_EQUIPMENT_ID_DF.append(
                        self.equipment_level_instrument_drift(offline_satatus, g_count, b_count, y_count, r_count, df),
                        ignore_index=True)
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc1_dt_el(self):
        """
        This will return the data and construct the data frame object
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_STABILITY_INDEX_DT.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)

            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = DEVIATION_TRACKER
                self.HC1_EQUIPMENT_DT_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc1_eh_el(self):
        """
        This will return the data and construct the data frame object for the equipment health
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_FURNACE_RUN_LENGTH.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)

            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = EQUIPMENT_HEALTH
                self.HC1_EQUIPMENT_EH_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc1_pt_el(self):
        """
        This will return the data and construct the data frame object for the plant performance tracker
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = PERFORMANCE_TRACKER
                self.HC1_EQUIPMENT_PT_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc1_db_el(self):
        """
                This will return the data and construct the data frame object for the Dynamic bench marking
                :return: None
                """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [COLOR_LBT.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)
            # Added the external target code
            queries_list_ext = [EXT_COLOR_LBT.format(self.sync_time)]
            ext_df = self.get_dataframe(queries_list_ext)
            if not ext_df.empty:
                self.HC1_EQUIPMENT_ET_DF = self.HC1_EQUIPMENT_ET_DF.append(ext_df, ignore_index=True)
            if not temp_df.empty:
                for furnace_name in temp_df.equipment_name.unique():
                    offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(
                        temp_df[temp_df.equipment_name == furnace_name])

                    self.HC1_EQUIPMENT_DB_DF = self.HC1_EQUIPMENT_DB_DF.append(
                        self.equipment_level_DB(offline_satatus, g_count, b_count, y_count, r_count, df),
                        ignore_index=True)
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    """
    Console Level Logic and implementation 
    """

    def get_data_for_hc1_id_cl(self):
        """
        This will return the data and construct the data frame object for the instrument drift
        :return: None
        """
        if not self.HC1_EQUIPMENT_ID_DF.empty:

            offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.HC1_EQUIPMENT_ID_DF)

            if r_count >= 1:
                self.HC1_EQUIPMENT_ID_DF[CONSOLE_FLAG] = RED_TAG

            elif y_count >= int(self.HC1_EQUIPMENT_ID_DF.shape[0] * .5):
                self.HC1_EQUIPMENT_ID_DF[CONSOLE_FLAG] = RED_TAG

            elif y_count >= int(self.HC1_EQUIPMENT_ID_DF.shape[0] * .3):
                self.HC1_EQUIPMENT_ID_DF[CONSOLE_FLAG] = YELLOW_TAG

    def get_data_for_hc1_dt_cl(self):
        """
        This will return the data and construct the data frame object for the deviation tacker
        :return: None
        """
        if not self.HC1_EQUIPMENT_DT_DF.empty:

            offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.HC1_EQUIPMENT_DT_DF)

            if r_count >= 5:
                self.HC1_EQUIPMENT_DT_DF[CONSOLE_FLAG] = RED_TAG

            elif 5 >= r_count >= 2:
                self.HC1_EQUIPMENT_DT_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif y_count >= 6 and r_count <= 2:
                self.HC1_EQUIPMENT_DT_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif y_count < 6:
                self.HC1_EQUIPMENT_DT_DF[CONSOLE_FLAG] = GREEN_TAG

            elif r_count < 2:
                self.HC1_EQUIPMENT_DT_DF[CONSOLE_FLAG] = GREEN_TAG

    def get_data_for_hc1_eh_cl(self):
        """
        This will return the data and construct the data frame object for the equipment health
        :return: None
        """

        if not self.HC1_EQUIPMENT_EH_DF.empty:

            offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.HC1_EQUIPMENT_EH_DF)

            if r_count >= 1:
                self.HC1_EQUIPMENT_EH_DF[CONSOLE_FLAG] = RED_TAG

            elif y_count >= int(self.HC1_EQUIPMENT_EH_DF.shape[0] * .5):
                self.HC1_EQUIPMENT_EH_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif int(self.HC1_EQUIPMENT_EH_DF.shape[0] * .5) >= y_count >= int(self.HC1_EQUIPMENT_EH_DF.shape[0] * .3):
                self.HC1_EQUIPMENT_EH_DF[CONSOLE_FLAG] = YELLOW_TAG

    def get_data_for_hc1_pt_cl(self):
        """
        This will return the data and construct the dataframe object for the performance tracker
        :return: None
        """

        if not self.HC1_EQUIPMENT_PT_DF.empty:

            offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.HC1_EQUIPMENT_PT_DF)

            if r_count >= 5:
                self.HC1_EQUIPMENT_PT_DF[CONSOLE_FLAG] = RED_TAG

            elif 5 >= r_count >= 2:
                self.HC1_EQUIPMENT_PT_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif y_count >= 6 and r_count <= 2:
                self.HC1_EQUIPMENT_PT_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif y_count <= 6:
                self.HC1_EQUIPMENT_PT_DF[CONSOLE_FLAG] = GREEN_TAG

            elif r_count <= 2:
                self.HC1_EQUIPMENT_PT_DF[CONSOLE_FLAG] = GREEN_TAG

    def get_data_for_hc1_db_cl(self):
        """
                This will return the data and construct the data frame object for the Dynamic bench marking
                :return: None
                """

        if not self.HC1_EQUIPMENT_DB_DF.empty:

            offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.HC1_EQUIPMENT_DB_DF)

            if r_count >= 1:
                self.HC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = RED_TAG

            elif y_count >= int(self.HC1_EQUIPMENT_DB_DF.shape[0] * .5):
                self.HC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = YELLOW_TAG

            elif int(self.HC1_EQUIPMENT_DB_DF.shape[0] * .5) >= y_count >= int(self.HC1_EQUIPMENT_DB_DF.shape[0] * .3):
                self.HC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = YELLOW_TAG


class TabularAndGraphColorCoding(HotConsole1):
    """
    This class is responsible to get the data and construct the in memory object for color coding philospy
    for Hot console1
    """

    def __init__(self):

        super().__init__()

        # Get and Initiate the equipment level coloring
        self.get_data_for_hc1_id_el()
        self.get_data_for_hc1_dt_el()
        self.get_data_for_hc1_eh_el()
        self.get_data_for_hc1_pt_el()
        self.get_data_for_hc1_db_el()
        # Initiate the console level coloring
        self.get_data_for_hc1_id_cl()
        self.get_data_for_hc1_dt_cl()
        self.get_data_for_hc1_eh_cl()
        self.get_data_for_hc1_pt_cl()
        self.get_data_for_hc1_db_cl()
        self.graph_df = self.get_empty_dataframe()
        self.tabular_df = self.get_empty_dataframe()

    def create_console_level_graph(self):
        """
        This will return the data and construct the data-frame object for equipment health
        :return: None
        """

        try:
            self.tabular_df = self.get_empty_dataframe()
            self.tabular_df = self.tabular_df.append([self.HC1_EQUIPMENT_EH_DF,
                                                      self.HC1_EQUIPMENT_DT_DF,
                                                      self.HC1_EQUIPMENT_ID_DF,
                                                      self.HC1_EQUIPMENT_PT_DF,
                                                      self.HC1_EQUIPMENT_DB_DF
                                                      ], ignore_index=True)

            if not self.tabular_df.empty:
                self.tabular_df[TIMESTAMP_KEY] = pd.to_datetime(self.tabular_df[TIMESTAMP_KEY], utc=True)
                self.tabular_df[TIMESTAMP_KEY] = self.tabular_df[TIMESTAMP_KEY].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                pass
            """
            Updating the graph values for the "Console level philosphy -EQMT " as per the color coding document 
            """

            status = None
            name_list = self.tabular_df[EQUIPMENT_NAME].unique()
            for name in name_list:
                df = self.tabular_df[self.tabular_df[EQUIPMENT_NAME] == name]
                # if RED_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = RED_TAG
                # elif self.tabular_df[self.tabular_df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3:
                #     # TODO : Condition need to be checked for the "atleast 3 out 5 flags need to be yellow
                #     # we are creating it as a yellow for a time being
                #     status = YELLOW_TAG
                # Added the new philosophy
                df_1 = self.HC1_EQUIPMENT_ET_DF[
                    self.HC1_EQUIPMENT_ET_DF['equipment_name'] == df['equipment_name'].iloc[0]]
                r_count = df_1[df_1[FLAG_STATUS_VALUE] == RED_TAG].shape[0]
                if df.feature.count() == 3:
                    df1 = pd.DataFrame({"alert_flag": [1, 1], "concern": [np.NaN, np.NaN],
                                        "console_flag": [df.console_flag.iloc[0], df.console_flag.iloc[0]],
                                        "console_name": [df.console_name.iloc[0], df.console_name.iloc[0]],
                                        "equipment_name": [df.equipment_name.iloc[0], df.equipment_name.iloc[0]],
                                        "feature": [np.NaN, np.NaN],
                                        "timestamp": [df.timestamp.iloc[0], df.timestamp.iloc[0]]})
                    df = pd.concat([df, df1])
                if df.feature.count() == 4:
                    df1 = pd.DataFrame({"alert_flag": [1], "concern": [np.NaN],
                                        "console_flag": [df.console_flag.iloc[0]],
                                        "console_name": [df.console_name.iloc[0]],
                                        "equipment_name": [df.equipment_name.iloc[0]],
                                        "feature": [np.NaN],
                                        "timestamp": [df.timestamp.iloc[0]]})
                    df = pd.concat([df, df1])
                if df.feature.count() == 2:
                    df1 = pd.DataFrame({"alert_flag": [1, 1,1], "concern": [np.NaN, np.NaN,np.NaN],
                                        "console_flag": [df.console_flag.iloc[0], df.console_flag.iloc[0],df.console_flag.iloc[0]],
                                        "console_name": [df.console_name.iloc[0], df.console_name.iloc[0],df.console_name.iloc[0]],
                                        "equipment_name": [df.equipment_name.iloc[0], df.equipment_name.iloc[0],df.equipment_name.iloc[0]],
                                        "feature": [np.NaN, np.NaN,np.NaN],
                                        "timestamp": [df.timestamp.iloc[0], df.timestamp.iloc[0],df.timestamp.iloc[0]]})
                    df = pd.concat([df, df1])
                if r_count:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] == 5:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 4 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 1:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 4 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 3 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 3 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 3 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 2:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 3:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3:
                    status = RED_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 4:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                    status = GREEN_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 3:
                    status = GREEN_TAG
                elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 5:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 4 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                    status = GREEN_TAG
                elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 2 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 3:
                    status = GREEN_TAG
                elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 1 and \
                        df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 4:
                    status = GREEN_TAG
                elif df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 5:
                    status = GREEN_TAG
                else:
                    status = GREEN_TAG

                self.graph_df = self.graph_df.append(
                    {
                        TIMESTAMP_KEY: df.timestamp.iloc[0],
                        CONSOLE_NAME_VALUE: df.console_name.iloc[0],
                        EQUIPMENT_NAME: df[EQUIPMENT_NAME].iloc[0],
                        FLAG_STATUS_VALUE: status,
                        CONSOLE_FLAG: None,
                        FEATURE_COL: None,
                    }, ignore_index=True)

            """
            Updating the graph values for the "Console level philosphy - Console " as per the color coding document 
            """

            name_list = self.graph_df[CONSOLE_NAME_VALUE].unique()

            for name in name_list:
                df = self.graph_df[self.graph_df[CONSOLE_NAME_VALUE] == name]

                # if RED_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = RED_TAG
                # elif self.graph_df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= int(
                #         self.graph_df.shape[0] * .5):
                #     status = RED_TAG
                # elif YELLOW_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = YELLOW_TAG
                # else:
                #     status = YELLOW_TAG
                # # TODO : Other condition needs to be checked as it is not explained !!
                if df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 7:
                    status = RED_TAG
                elif 7 >= df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 4 and \
                        df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 7:
                    status = RED_TAG
                elif 7 >= df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 3:
                    status = YELLOW_TAG
                elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] < 3:
                    status = GREEN_TAG
                else:
                    status = None

                self.graph_df[CONSOLE_FLAG] = status
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))


class UnitLevel(TabularAndGraphColorCoding):
    """
    This class is responsible to get the data and construct the in memory object for color coding philospy
    for unit level
    """

    def __init__(self):
        """
        This will initiate the class and get the data for the respective console
        """

        super().__init__()

        # Inititate the console level coloring for graphs
        self.create_console_level_graph()
        if not self.graph_df.empty:
            self._psql_session.execute(""" TRUNCATE TABLE color_coding_graph  """)
            execute_values(self._psql_session, """
                               INSERT INTO public.color_coding_graph(
                               "timestamp", feature, console_name, equipment_name, console_flag, alert_flag , concern)
                               VALUES %s;
                               """, self.graph_df.to_records(index=False).tolist())
            self.connection.commit()

        else:
            pass
        if not self.tabular_df.empty:
            self._psql_session.execute(""" TRUNCATE TABLE color_coding_tabular  """)
            self.tabular_df = self.tabular_df[
                ["alert_flag", "console_flag", "console_name", "equipment_name", "feature", "timestamp", "concern"]]
            execute_values(self._psql_session, """ 
            INSERT INTO public.color_coding_tabular(
            alert_flag, console_flag,console_name, equipment_name, feature, "timestamp" , concern) VALUES %s; """,
                           self.tabular_df.to_records(index=False).tolist())
            self.connection.commit()
        else:
            pass


def get_hot_console1_color_coding_values():
    # *args, **kwargs
    obj = None

    try:
        UnitLevel()
    except Exception as e:
        log_error("Exception due to : %s" + str(e))

    finally:
        if obj:
            del obj
