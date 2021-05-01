"""
File                :   cold console1.py

Description         :   This will handle the color code philosophy for unit ,
                        console , equipment and feature level

Author              :   LivNSense Technologies

Date Created        :   20-11-2019

Date Last modified :    20-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
from color_coding.hotconsole2 import *


class ColdConsole1(ColorCodingUtility):
    """
    This class is responsible to get the data and construct the in memory object for color coding philospy
    for Cold console 1
    """

    def __init__(self):

        super().__init__()
        self.CC1_EQUIPMENT_PT_DF = self.get_empty_dataframe()
        self.CC1_EQUIPMENT_DB_DF = self.get_empty_dataframe()
        self.CC1_EQUIPMENT_DT_DF = self.get_empty_dataframe()
        self.CC1_EQUIPMENT_ID_DF = self.get_empty_dataframe()
        self.CC1_EQUIPMENT_EH_DF = self.get_empty_dataframe()
        self.CC1_EQUIPMENT_ET_DF = self.get_empty_dataframe()

    @classmethod
    def feature_level_equipment_health(cls, offline_status, g_count, b_count, y_count, r_count, df):

        status = None
        if df['equipment_name'].iloc[0] == 'GB-201':
            if 3 >= r_count >= 5:
                status = RED_TAG
            elif 5 >= r_count >= 1 or 5 >= r_count >= 2:
                status = YELLOW_TAG
            else:
                status = GREEN_TAG
        elif df['equipment_name'].iloc[0] == 'GB-202':
            if 4 >= r_count >= 2:
                status = RED_TAG
            elif 4 >= r_count >= 1:
                status = YELLOW_TAG
            else:
                status = GREEN_TAG
        elif df['equipment_name'].iloc[0] == 'DA-203':
            if 1 >= r_count >= 1:
                status = RED_TAG
            elif 2 >= r_count >= 1:
                status = YELLOW_TAG
            else:
                status = GREEN_TAG
        else:
            pass

        # status = None
        return {TIMESTAMP_KEY: df.timestamp.iloc[0],
                CONSOLE_NAME_VALUE: df.console_name.iloc[0],
                EQUIPMENT_NAME: df.equipment_name.iloc[0],
                FLAG_STATUS_VALUE: status,
                CONSOLE_FLAG: None,
                FEATURE_COL: EQUIPMENT_HEALTH,
                }

    def get_data_for_cc1_db_el(self):
        """
                This will return the data and construct the data frame object for the Dynamic bench marking
                :return: None
                """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [NON_FURNACE_COLOR_LBT.format(self.sync_time, COLD_CONSOLE_1_VALUE)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:

                for furnace_name in temp_df.equipment_name.unique():
                    offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(
                        temp_df[temp_df.equipment_name == furnace_name])

                    self.CC1_EQUIPMENT_DB_DF = self.CC1_EQUIPMENT_DB_DF.append(
                        self.equipment_level_DB(offline_satatus, g_count, b_count, y_count, r_count, df),
                        ignore_index=True)

            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_cc1_pt_el(self):
        """
        This will return the data and construct the data frame object for the Cold console 1 plant performance tracker
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_FURNACE_PERFORMANCE_TRACKER_QUERY_COLD_CONSOLE_1.format(self.sync_time)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = PERFORMANCE_TRACKER
                self.CC1_EQUIPMENT_PT_DF = temp_df.copy()

            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_cc1_dt_el(self):
        """
        This will return the data and construct the data frame object
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_NON_FURNACE_STABILITY_DT.format(self.sync_time, COLD_CONSOLE_1_VALUE)]

            temp_df = self.get_dataframe(queries_list)

            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = DEVIATION_TRACKER
                self.CC1_EQUIPMENT_DT_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_cc1_eh_el(self):
        """
                This will return the data and construct the data frame object for the Equipment_health
                :return: None
                """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [FEATURE_LEVEL_EQUIP_HEALTH_CC1.format(self.sync_time, COLD_CONSOLE_1_VALUE)]

            temp_df = self.get_dataframe(queries_list)

            if not temp_df.empty:

                for furnace_name in temp_df.equipment_name.unique():
                    offline_status, g_count, b_count, y_count, r_count, df = self.get_color_count(
                        temp_df[temp_df.equipment_name == furnace_name])

                    self.CC1_EQUIPMENT_EH_DF = self.CC1_EQUIPMENT_EH_DF.append(
                        self.feature_level_equipment_health(offline_status, g_count, b_count, y_count, r_count, df),
                        ignore_index=True)
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_cc1_id_el(self):
        """
                      This will return the data and construct the data frame object for the Instrument Drift
                      :return: Noness
                    """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [FEATURE_LEVEL_INSTRUMENT_DRIFT_CC1.format(self.sync_time, COLD_CONSOLE_1_VALUE)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = INSTRUMENT_DRIFT
                self.CC1_EQUIPMENT_ID_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    # def get_data_for_cc1_db_cl(self):
    #
    #     if not self.CC1_EQUIPMENT_DB_DF.empty:
    #         offline_satatus, g_count, b_count, y_count, r_count, _ = self.get_color_count(self.CC1_EQUIPMENT_DB_DF)
    #         if r_count >= 1:
    #             self.CC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = RED_TAG
    #         elif 6 >= r_count >= 4:
    #             self.CC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = YELLOW_TAG
    #         elif y_count >= 1:
    #             self.CC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = YELLOW_TAG
    #         elif 2 >= r_count:
    #             self.CC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = RED_TAG
    #         elif 2 >= y_count:
    #             self.CC1_EQUIPMENT_DB_DF[CONSOLE_FLAG] = YELLOW_TAG


class Cold_console1_TabularAndGraphColorCoding(ColdConsole1, TabularAndGraphColorCoding):

    def __init__(self):
        super().__init__()
        self.get_data_for_cc1_db_el()
        self.get_data_for_cc1_pt_el()
        self.get_data_for_cc1_dt_el()
        self.get_data_for_cc1_eh_el()
        self.get_data_for_cc1_id_el()
        self.cc1_f_tabular = self.get_empty_dataframe()
        self.cc1_f_graph_df = self.get_empty_dataframe()
        self.cc1_f_tabular = self.get_empty_dataframe()
        self.create_console_level_graph()
        # self.Non_Furnace_console_level_graph()

    def cold_console1_Furnace_console_level_graph(self):
        """
            This will return the data and construct the data-frame objects of graph and tabular for
            Cold console1  non furnaces
            :return: None
            """

        try:

            self.cc1_f_tabular = self.get_empty_dataframe()
            self.cc1_f_tabular = self.cc1_f_tabular.append([
                self.CC1_EQUIPMENT_DB_DF,
                self.CC1_EQUIPMENT_PT_DF,
                self.CC1_EQUIPMENT_DT_DF,
                self.CC1_EQUIPMENT_EH_DF,
                self.CC1_EQUIPMENT_ID_DF
            ], ignore_index=True)

            if not self.cc1_f_tabular.empty:
                self.cc1_f_tabular[TIMESTAMP_KEY] = pd.to_datetime(self.cc1_f_tabular[TIMESTAMP_KEY], utc=True)
                self.cc1_f_tabular[TIMESTAMP_KEY] = self.cc1_f_tabular[TIMESTAMP_KEY].dt.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            else:
                pass
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [COLOR_CODING_EQUIPMENT_LEVEL_CC1.format(self.sync_time, COLD_CONSOLE_1_VALUE)]
            cc1_f_tabular = self.get_dataframe(queries_list)
            if not cc1_f_tabular.empty:
                self.CC1_EQUIPMENT_ET_DF = self.CC1_EQUIPMENT_ET_DF.append(cc1_f_tabular, ignore_index=True)
            status = None
            name_list = self.cc1_f_tabular[EQUIPMENT_NAME].unique()
            for name in name_list:
                df = self.cc1_f_tabular[self.cc1_f_tabular[EQUIPMENT_NAME] == name]
                if df['equipment_name'].iloc[0] in CC1_EQUIPMENT_LIST:
                    df_1 = self.CC1_EQUIPMENT_ET_DF[
                        self.CC1_EQUIPMENT_ET_DF['equipment_name'] == df['equipment_name'].iloc[0]]
                    r_count = df_1[df_1[FLAG_STATUS_VALUE] == RED_TAG].shape[0]
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
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2:
                        status = RED_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 3:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 1 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 3:
                        status = RED_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 4:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 3 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                        status = GREEN_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 1 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 3:
                        status = GREEN_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 5:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 4 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 2:
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
                else:
                    if df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] == 5:
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
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2:
                        status = RED_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 3:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 1 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 2 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 3:
                        status = RED_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 4:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 3 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 2 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 2:
                        status = GREEN_TAG
                    elif df[df[FLAG_STATUS_VALUE] == RED_TAG].shape[0] >= 1 and \
                            df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[
                                0] >= 1 and df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[0] >= 3:
                        status = GREEN_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 5:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 4 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 1:
                        status = YELLOW_TAG
                    elif df[df[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3 and \
                            df[df[FLAG_STATUS_VALUE] == GREEN_TAG].shape[
                                0] >= 2:
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
                # if RED_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = RED_TAG
                # elif self.non_f_tabular[self.non_f_tabular[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3:
                #     # TODO : Condition need to be checked for the "at least 3 out 5 flags need to be yellow
                #     # we are creating it as a yellow for a time being
                #     status = YELLOW_TAG
                # else:
                #     status = None
                self.cc1_f_graph_df = self.cc1_f_graph_df.append(
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
                name_list = self.cc1_f_graph_df[CONSOLE_NAME_VALUE].unique()
                for name in name_list:
                    df = self.cc1_f_graph_df[self.cc1_f_graph_df[CONSOLE_NAME_VALUE] == name]
                    critical = ['GB-201', 'GB-202', 'GB-325', 'DA-301', 'GB-501', 'GB-601']
                    critical_ep = df[df['equipment_name'].isin(critical)]
                    non_critical_ep = df[~df['equipment_name'].isin(critical)]
                    offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(critical_ep)
                    non_offline_satatus, non_g_count, non_b_count, non_y_count, non_r_count, non_df = self.get_color_count(
                        non_critical_ep)
                    if r_count >= 3:
                        status = RED_TAG
                    elif r_count >= 2 and r_count >= 3:
                        status = RED_TAG
                    elif r_count >= 2 and non_r_count >= 2:
                        status = RED_TAG
                    elif r_count >= 2 and non_r_count < 2:
                        status = YELLOW_TAG
                    elif r_count >= 1:
                        status = GREEN_TAG
                    else:
                        status = GREEN_TAG
                    # pass
                    # elif YELLOW_TAG in df[FLAG_STATUS_VALUE].values:
                    #     status = YELLOW_TAG
                    # else:
                    #     status = YELLOW_TAG
                    # # TODO : Other condition needs to be checked as it is not explained !!
                    self.cc1_f_graph_df[CONSOLE_FLAG] = status
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))


class UnitLevel(Cold_console1_TabularAndGraphColorCoding):
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
        # self.create_console_level_graph()
        # self.Non_Furnace_console_level_graph()
        self.cold_console1_Furnace_console_level_graph()
        if not self.graph_df.empty:
            if not self.cc1_f_graph_df.empty:
                # self._psql_session.execute(""" TRUNCATE TABLE color_coding_graph  """)

                execute_values(self._psql_session, """
                                   INSERT INTO public.color_coding_graph(
                                   "timestamp", feature, console_name, equipment_name, console_flag, alert_flag , concern)
                                   VALUES %s;
                                   """, self.cc1_f_graph_df.to_records(index=False).tolist())

                self.connection.commit()

        else:
            pass
        if not self.tabular_df.empty:
            if not self.cc1_f_tabular.empty:
                self.cc1_f_tabular = self.cc1_f_tabular[
                    ["alert_flag", "console_flag", "console_name", "equipment_name", "feature", "timestamp", "concern"]]
                execute_values(self._psql_session, """INSERT INTO public.color_coding_tabular( alert_flag,
                     console_flag,console_name, equipment_name, feature, "timestamp" , concern) VALUES %s; """,
                               self.cc1_f_tabular.to_records(index=False).tolist())
                self.connection.commit()
        else:
            pass


def get_cold_console1_color_coding_values():
    # *args, **kwargs
    obj = None

    try:
        UnitLevel()
    except Exception as e:
        log_error("Exception due to : %s" + str(e))

    finally:
        if obj:
            del obj
