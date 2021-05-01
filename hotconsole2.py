"""
File                :   hot console2.py

Description         :   This will handle the color code philosophy for unit ,
                        console , equipment and feature level

Author              :   LivNSense Technologies

Date Created        :   20-11-2019

Date Last modified :    20-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
from color_coding.hotconsole1 import *


class HotConsole2(ColorCodingUtility):
    """
    This class is responsible to get the data and construct the in memory object for color coding philospy
    for hot console 2
    """

    def __init__(self):

        super().__init__()

        self.HC2_EQUIPMENT_DB_DF = self.get_empty_dataframe()
        self.HC2_EQUIPMENT_DT_DF = self.get_empty_dataframe()
        self.HC2_EQUIPMENT_EH_DF = self.get_empty_dataframe()
        self.HC2_EQUIPMENT_ET_DF = self.get_empty_dataframe()

    def get_data_for_hc2_db_el(self):
        """
                This will return the data and construct the data frame object for the Dynamic bench marking
                :return: None
                """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            queries_list = [NON_FURNACE_COLOR_LBT.format(self.sync_time, HOT_CONSOLE_2_VALUE)]

            temp_df = self.get_dataframe(queries_list)
            queries_list = [NON_FURNACE_EXTERNAL_COLOR_LBT.format(self.sync_time, HOT_CONSOLE_2_VALUE)]
            ext_df = self.get_dataframe(queries_list)
            if not ext_df.empty:
                self.HC2_EQUIPMENT_ET_DF = self.HC2_EQUIPMENT_ET_DF.append(ext_df, ignore_index=True)
            if not temp_df.empty:
                for furnace_name in temp_df.equipment_name.unique():
                    offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(
                        temp_df[temp_df.equipment_name == furnace_name])

                    self.HC2_EQUIPMENT_DB_DF = self.HC2_EQUIPMENT_DB_DF.append(
                        self.equipment_level_DB(offline_satatus, g_count, b_count, y_count, r_count, df),
                        ignore_index=True)
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc2_dt_el(self):
        """
        This will return the data and construct the data frame object
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_NON_FURNACE_STABILITY_DT.format(self.sync_time, HOT_CONSOLE_2_VALUE)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = DEVIATION_TRACKER
                self.HC2_EQUIPMENT_DT_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))

    def get_data_for_hc2_eh_el(self):
        """
        This will return the data and construct the data frame object for the equipment health
        :return: None
        """

        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            queries_list = [COLOR_CODING_EH_HC2.format(HOT_CONSOLE_2_VALUE, self.sync_time)]

            temp_df = self.get_dataframe(queries_list)
            if not temp_df.empty:
                temp_df[CONSOLE_FLAG] = None
                temp_df[FEATURE_COL] = EQUIPMENT_HEALTH
                self.HC2_EQUIPMENT_EH_DF = temp_df.copy()
            else:
                pass
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))


class Hot_Console2_TabularAndGraphColorCoding(HotConsole2, TabularAndGraphColorCoding):
    def __init__(self):

        super().__init__()
        self.get_data_for_hc2_db_el()
        self.get_data_for_hc2_dt_el()
        self.get_data_for_hc2_eh_el()
        self.non_f_graph_df = self.get_empty_dataframe()
        self.non_f_tabular = self.get_empty_dataframe()
        self.create_console_level_graph()

    def Non_Furnace_console_level_graph(self):
        """
            This will return the data and construct the data-frame objects of graph and tabular for
            Hot console2  non furnaces
            :return: None
            """

        try:
            self.non_f_tabular = self.get_empty_dataframe()
            self.non_f_tabular = self.non_f_tabular.append([

                self.HC2_EQUIPMENT_DB_DF,
                self.HC2_EQUIPMENT_DT_DF,
                self.HC2_EQUIPMENT_EH_DF
            ], ignore_index=True)

            if not self.non_f_tabular.empty:
                self.non_f_tabular[TIMESTAMP_KEY] = pd.to_datetime(self.non_f_tabular[TIMESTAMP_KEY], utc=True)
                self.non_f_tabular[TIMESTAMP_KEY] = self.non_f_tabular[TIMESTAMP_KEY].dt.strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            else:
                pass
            """
            Updating the graph values for the "Console level philosphy -EQMT " as per the color coding document 
         """

            status = None
            name_list = self.non_f_tabular[EQUIPMENT_NAME].unique()
            for name in name_list:
                df = self.non_f_tabular[self.non_f_tabular[EQUIPMENT_NAME] == name]
                if df['equipment_name'].iloc[0] in ['DA-101', 'DA-103', 'FA-116']:
                    df_1 = self.HC2_EQUIPMENT_ET_DF[
                        self.HC2_EQUIPMENT_ET_DF['equipment_name'] == df['equipment_name'].iloc[0]]
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

                # if RED_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = RED_TAG
                # elif self.non_f_tabular[self.non_f_tabular[FLAG_STATUS_VALUE] == YELLOW_TAG].shape[0] >= 3:
                #     # TODO : Condition need to be checked for the "at least 3 out 5 flags need to be yellow
                #     # we are creating it as a yellow for a time being
                #     status = YELLOW_TAG
                # else:
                #     status = None
                self.non_f_graph_df = self.non_f_graph_df.append(
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

            name_list = self.non_f_graph_df[CONSOLE_NAME_VALUE].unique()

            for name in name_list:
                df = self.non_f_graph_df[self.non_f_graph_df[CONSOLE_NAME_VALUE] == name]
                critical = ['DA-101', 'DA-103']
                critical_ep = df[df['equipment_name'].isin(critical)]
                non_critical_ep = df[~df['equipment_name'].isin(critical)]
                offline_satatus, g_count, b_count, y_count, r_count, df = self.get_color_count(critical_ep)
                non_offline_satatus, non_g_count, non_b_count, non_y_count, non_r_count, non_df = self.get_color_count(
                    non_critical_ep)
                if r_count >= 2:
                    status = RED_TAG
                elif r_count >= 1 and non_r_count >= 4:
                    status = RED_TAG
                elif r_count >= 1 and non_r_count < 4:
                    status = YELLOW_TAG
                elif r_count >= 1:
                    status = GREEN_TAG
                elif non_r_count >= 4:
                    status = YELLOW_TAG
                elif non_r_count < 4:
                    status = GREEN_TAG
                else:
                    status = GREEN_TAG
                # elif YELLOW_TAG in df[FLAG_STATUS_VALUE].values:
                #     status = YELLOW_TAG
                # else:
                #     status = YELLOW_TAG
                # # TODO : Other condition needs to be checked as it is not explained !!

                self.non_f_graph_df[CONSOLE_FLAG] = status
        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))


class UnitLevel(Hot_Console2_TabularAndGraphColorCoding):
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
        self.Non_Furnace_console_level_graph()
        if not self.graph_df.empty:
            if not self.non_f_graph_df.empty:
                execute_values(self._psql_session, """
                                   INSERT INTO public.color_coding_graph(
                                   "timestamp", feature, console_name, equipment_name, console_flag, alert_flag , concern)
                                   VALUES %s;
                                   """, self.non_f_graph_df.to_records(index=False).tolist())
                self.connection.commit()

        else:
            pass
        if not self.tabular_df.empty:
            if not self.non_f_tabular.empty:
                self.non_f_tabular = self.non_f_tabular[
                    ["alert_flag", "console_flag", "console_name", "equipment_name", "feature", "timestamp", "concern"]]
                execute_values(self._psql_session, """INSERT INTO public.color_coding_tabular( alert_flag,
                     console_flag,console_name, equipment_name, feature, "timestamp" , concern) VALUES %s; """,
                               self.non_f_tabular.to_records(index=False).tolist())
                self.connection.commit()
        else:
            pass


def get_hot_console2_color_coding_values():
    # *args, **kwargs
    obj = None

    try:
        UnitLevel()
    except Exception as e:
        log_error("Exception due to : %s" + str(e))

    finally:
        if obj:
            del obj
