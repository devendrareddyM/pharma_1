"""
File                :   equipment_health.py

Description         :   This will return the Equipment Health value for the particular console
                        and equipment name

Author              :   LivNSense Technologies

Date Created        :   13-08-2019

Date Last modified :    6-12-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import datetime
import json
import time as t
from urllib.parse import parse_qs
from datetime import datetime
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


class EquipmentHealth(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and respond for the equipment health value
    """

    def __init__(self, query_parms=None, unit=None, console=None, equipment=None):
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
        self.query_parms = query_parms

    def compose_dict_object(self):
        equipment_health_status = GREEN_TAG
        tag_name = []
        tag_value = []
        dol = []
        self.sync_time = None
        self._psql_session.execute(EQ_CLR_GRAPH)
        timetsamp_record = self._psql_session.fetchone()
        self._psql_session.execute("select * from color_coding_tabular where equipment_name = '{}' and feature = '{}' ".format(self.equipment,EQUIPMENT_HEALTH))
        clr_tabular_data = self._psql_session.fetchone()
        self.sync_time = timetsamp_record[TIMESTAMP_KEY]

        if self.console == HOT_CONSOLE_1_VALUE:
            dict_data = {
                TIMESTAMP_KEY: self.sync_time,
                "FDHDR_TAG": tag_name,
                "FDHDR_VALUE": tag_value,
                "DOL_VALUE": dol,
                "status": equipment_health_status,
                "data": []
            }
            return dict_data

        else:
                dict_data = {
                    TIMESTAMP_KEY: self.sync_time,
                    "status": equipment_health_status,
                    "data": []
                }
                return dict_data




    def get_color_coding_tabular(self, dict_data):
        self.sync_time = None

        COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}' and " \
                               "feature = '{}' "

        # todo : respond back the empty json reponse
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()

        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )

        self.sync_time = timetsamp_record[TIMESTAMP_KEY]

        curr_time = t.time()
        curr_time = int(curr_time - (curr_time % 60) - 120) * 1000

        """ === """
        """
        Color Coding for tabular data
        """

        try:
            self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment, EQUIPMENT_HEALTH))

            tabular_df = pd.DataFrame(self._psql_session.fetchall())

            if not tabular_df.empty:
                try:
                    self.sync_time = str(tabular_df.timestamp.iloc[0])
                    dict_data["status"] = int(tabular_df[FLAG_STATUS_VALUE].iloc[0])
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
            dict_data['FDHDR_TAG'] = tag_name
            dict_data['FDHDR_VALUE'] = tag_value
            dict_data["DOL_VALUE"] = dol
        else:
            pass
        if fdhr_value not in CHECK_FDHDR_VALUE and self.console == HOT_CONSOLE_1_VALUE:
            if self.console == HOT_CONSOLE_1_VALUE:
                dict_data["DOL_VALUE"] = None
                dict_data["status"] = 0
                dict_data["data"] = []
        elif self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE or self.console == COLD_CONSOLE_2_VALUE :
            dict_data["DOL_VALUE"] = None

    def get_equipment_health_data(self, dict_data):
        if self.console == HOT_CONSOLE_1_VALUE:

            if self.query_parms:
                if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                    self._psql_session.execute(
                        DETAILED_FURNACE_RUN_LENGTH_GRAPH_NULL_START_DATE.format(
                            self.equipment,
                            self.query_parms["end_date"][0]))
                else:
                    self._psql_session.execute(
                        DETAILED_FURNACE_RUN_LENGTH_GRAPH.format(
                            self.equipment,
                            self.query_parms["start_date"][0],
                            self.query_parms["end_date"][0]))

                df_data = pd.DataFrame(self._psql_session.fetchall())

                graph = []

                if not df_data.empty:
                    df_data = df_data.where(pd.notnull(df_data) == True, None)
                    df_data.sort_values("timestamp", ascending=True, inplace=True)

                    COIL_1 = df_data[df_data["coil"] == 1]
                    COIL_2 = df_data[df_data["coil"] == 2]
                    COIL_3 = df_data[df_data["coil"] == 3]
                    COIL_4 = df_data[df_data["coil"] == 4]
                    COIL_5 = df_data[df_data["coil"] == 5]
                    COIL_6 = df_data[df_data["coil"] == 6]

                    temp = {"coil 1": list(COIL_1["predicted_coil_dol"]),
                            "coil 2": list(COIL_2["predicted_coil_dol"]),
                            "coil 3": list(COIL_3["predicted_coil_dol"]),
                            "coil 4": list(COIL_4["predicted_coil_dol"])}

                    if self.equipment != 'BA-113':
                        temp["coil 5"] = list(COIL_5["predicted_coil_dol"])
                        temp["coil 6"] = list(COIL_6["predicted_coil_dol"])

                    temp["x_axis"] = list(COIL_1["timestamp"])
                    graph.append(temp)
                    dict_data["graph_data"] = graph

                self._psql_session.execute(
                    DETAILED_FURNACE_RUN_LENGTH_ID.format(self.equipment, self.sync_time))

                df_data = pd.DataFrame(self._psql_session.fetchall())

                if not df_data.empty:
                    df_data.sort_values("coil", inplace=True)
                    df_data["equipment_name"] = [str(self.equipment) + " Coil #" + str(val) for val in df_data[
                        "coil"]]

                    temp_df = pd.DataFrame([{"equipment_name": self.equipment,
                                             "timestamp": df_data.timestamp.iloc[0],
                                             "description": self.equipment + " Pred DOL",
                                             "actual_dol": df_data.actual_dol.iloc[0],
                                             "alert_flag": df_data.alert_flag.iloc[0],
                                             "predicted_coil_dol": df_data.predicted_dol.iloc[0],
                                             }])

                    df_data["actual_dol"] = None
                    df_data['alert_flag'] = df_data['coil_alert_flag']
                    df_data.rename(columns={'coil_alert_flag': 'alert_flag'})
                    df_data.drop(columns='coil_alert_flag')

                    temp_df = temp_df.append(df_data, ignore_index=True)
                    dict_data["data"] = yaml.safe_load(temp_df.to_json(orient=RECORDS))
                if df_data.empty:
                    dict_data["status"] = 0

                return JsonResponse(dict_data, safe=False)

        elif self.console == HOT_CONSOLE_2_VALUE:
            """
            Equipment Health for Ethane Surge Drum
            """

            if self.equipment == 'FA-155':
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            START_DATE_NULL.format(
                                self.equipment,
                                self.query_parms["end_date"][0]))
                    else:

                        self._psql_session.execute(
                            DETAILED_PCA_QUERY.format(
                                self.equipment,
                                self.query_parms[
                                    "start_date"][0],
                                self.query_parms[
                                    "end_date"][0]))
                    df_data = pd.DataFrame(self._psql_session.fetchall())

                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values("timestamp", ascending=True, inplace=True)

                        df_temp = df_data[df_data["equipment_part"] == "EA-155"]
                        temp = {}
                        if not df_temp.empty:
                            temp["actual"] = list(df_temp["tag_value"])
                            temp["predicted"] = list(df_temp["predicted_value"])
                            temp["x_axis"] = list(df_temp["timestamp"])
                            temp["alert_tag"] = list(df_temp["alert_flag"])
                            temp["equipment_part"] = str(df_temp["equipment_part"].iloc[0])
                            temp["unit"] = str(df_temp["unit"].iloc[0])
                            graph.append(temp)
                    dict_data["graph_data"] = graph
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1

                    return JsonResponse(dict_data, safe=False)

        elif self.console == COLD_CONSOLE_1_VALUE:
            """
            Equipment Health for Charge Gas Compressor-1
            """
            """
            Equipment Helath for 203
            """

            if self.equipment == 'DA-203':
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_MONITORING_QUERY_DA_203_START_NULL.format(self.equipment,
                                                                                                self.query_parms[
                                                                                                    "end_date"][0]))

                    else:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_MONITORING_QUERY_DA_203.format(self.equipment,
                                                                                     self.query_parms["start_date"][0],
                                                                                     self.query_parms["end_date"][0]))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values("timestamp", ascending=True, inplace=True)
                        df_temp = df_data[df_data["equipment_part"] == "EA-212"]
                        temp = {}
                        if not df_temp.empty:
                            temp["actual"] = list(df_data["tag_value"])
                            temp["predicted"] = list(df_data["predicted_value"])
                            temp["x_axis"] = list(df_data["timestamp"])
                            temp["alert_tag"] = list(df_data["alert_flag"])
                            temp["equipment_part"] = str(df_data["equipment_part"].iloc[0])
                            temp["unit"] = str(df_data["unit"].iloc[0])
                            graph.append(temp)
                    dict_data["graph_data"] = graph
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1
                    return JsonResponse(dict_data, safe=False)

            if self.equipment == 'GB-201':
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            START_DATE_NULL.format(
                                self.equipment,
                                self.query_parms[
                                    "end_date"][0]))

                    else:
                        self._psql_session.execute(
                            DETAILED_PCA_QUERY.format(
                                self.equipment,
                                self.query_parms[
                                    "start_date"][0],
                                self.query_parms[
                                    "end_date"][0]))
                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values("timestamp", ascending=True, inplace=True)
                        equipment_part_list = df_data.equipment_part.unique()
                        for part in equipment_part_list:
                            temp = {}
                            df_temp = df_data[df_data.equipment_part == part]
                            if not df_temp.empty:
                                temp["actual"] = list(df_temp["tag_value"])
                                temp["predicted"] = list(df_temp["predicted_value"])
                                temp["x_axis"] = list(df_temp["timestamp"])
                                temp["alert_tag"] = list(df_temp["alert_flag"])
                                temp["unit"] = str(df_data["unit"].iloc[0])
                                temp["equipment_part"] = part
                                graph.append(temp)
                    dict_data["graph_data"] = graph
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1
                    return JsonResponse(dict_data, safe=False)

            """
            Equipment Health for Charge Gas Compressor-2

            """

            if self.equipment == 'GB-202':
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            START_DATE_NULL.format(
                                self.equipment,
                                self.query_parms[
                                    "end_date"][0]))
                    else:
                        self._psql_session.execute(
                            DETAILED_PCA_QUERY.format(
                                self.equipment,
                                self.query_parms[
                                    "start_date"][0],
                                self.query_parms[
                                    "end_date"][0]))
                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values("timestamp", ascending=True, inplace=True)
                        equipment_part_list = df_data.equipment_part.unique()
                        for part in equipment_part_list:
                            temp = {}
                            df_temp = df_data[df_data.equipment_part == part]
                            if not df_temp.empty:
                                temp["actual"] = list(df_temp["tag_value"])
                                temp["predicted"] = list(df_temp["predicted_value"])
                                temp["x_axis"] = list(df_temp["timestamp"])
                                temp["alert_tag"] = list(df_temp["alert_flag"])
                                temp["unit"] = str(df_data["unit"].iloc[0])
                                temp["equipment_part"] = part
                                graph.append(temp)
                    dict_data["graph_data"] = graph
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1
                    return JsonResponse(dict_data, safe=False)

            return JsonResponse([], safe=False)

        elif self.console == COLD_CONSOLE_2_VALUE:

            """
            Equipment Health for C2 Splitter
            """

            if self.equipment == 'DA-403':
                self._psql_session.execute(DEATILED_C2_SPLITTER_GRAPH_EH.format(self.equipment))
                df_graph = pd.DataFrame(self._psql_session.fetchall())

                self._psql_session.execute(DEATILED_C2_SPLITTER_RESULT_EH.format(self.equipment))
                df_data = pd.DataFrame(self._psql_session.fetchall())
                graph_data = []
                if not df_graph.empty and not df_data.empty:

                    tray_uni_list = df_graph.tray_tag.unique()

                    df_graph = df_graph.where(pd.notnull(df_graph) == True, None)

                    for tray in tray_uni_list:
                        temp = {}
                        df_temp = df_graph[df_graph.tray_tag == tray]

                        temp["tray_name"] = tray

                        temp["op_liquid_load"] = df_data[df_data["concern"] == tray]["op_liquid_load"].iloc[0]
                        temp["op_vapour_load"] = df_data[df_data["concern"] == tray]["op_vapour_load"].iloc[0]

                        temp[FLAG_STATUS_VALUE] = int(
                            df_data[df_data["concern"] == tray][FLAG_STATUS_VALUE].iloc[0])

                        temp["min_vapour_x"] = list(df_temp["min_vapour_x"])
                        temp["min_vapour_y"] = list(df_temp["min_vapour_y"])

                        temp["max_vapour_x"] = list(df_temp["max_vapour_x"])
                        temp["max_vapour_y"] = list(df_temp["max_vapour_y"])

                        temp["dc_backup_x"] = list(df_temp["dc_backup_x"])
                        temp["dc_backup_y"] = list(df_temp["dc_backup_y"])

                        temp["min_liquid_x"] = list(df_temp["min_liquid_x"])
                        temp["min_liquid_y"] = list(df_temp["min_liquid_y"])

                        temp["max_liquid_x"] = list(df_temp["max_liquid_x"])
                        temp["max_liquid_y"] = list(df_temp["max_liquid_y"])

                        temp["constant_lv_x"] = list(df_temp["constant_lv_x"])
                        temp["constant_lv_y"] = list(df_temp["constant_lv_y"])

                        graph_data.append(temp)
                    dict_data["graph_data"] = graph_data
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1
                    return JsonResponse(dict_data, safe=False)
                return JsonResponse([], safe=False)

            """ Equipment Health for primary c3 splitter  """
            if self.equipment == 'DA-406':
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_EH_START_NULL.format(
                                self.equipment,
                                self.query_parms[
                                    "end_date"][0]))
                    else:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_EH.format(
                                self.equipment, self.query_parms["start_date"][0], self.query_parms["end_date"][0]))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    graph = []

                    if not df_data.empty:
                        df_data.sort_values("timestamp", ascending=True, inplace=True)

                        df_temp = df_data[df_data["equipment_part"] == "EA-418A"]
                        temp = {}
                        if not df_temp.empty:
                            temp["y_axis"] = list(df_temp["eh_ea_ua"])
                            temp["x_axis"] = list(df_temp["timestamp"])
                            temp[FLAG_STATUS_VALUE] = list(df_temp[FLAG_STATUS_VALUE])
                            temp["equipment_part"] = str(df_temp["equipment_part"].iloc[0])
                            graph.append(temp)

                        df_temp = df_data[df_data["equipment_part"] == "EA-418B"]
                        temp = {}
                        if not df_temp.empty:
                            temp["y_axis"] = list(df_temp["eh_ea_ua"])
                            temp["x_axis"] = list(df_temp["timestamp"])
                            temp[FLAG_STATUS_VALUE] = list(df_temp[FLAG_STATUS_VALUE])
                            temp["equipment_part"] = str(df_temp["equipment_part"].iloc[0])
                            graph.append(temp)

                    dict_data["graph_data"] = graph
                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1
                    return JsonResponse(dict_data, safe=False)

            """
            Equipment Health for PP Deethanizer (DA-480) and Secondary C3 Splitter (DA-490)
            """
            if self.equipment in ['DA-480', 'DA-490']:
                if self.query_parms:
                    if 'start_date' not in self.query_parms or not self.query_parms["start_date"]:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_EH_START_NULL.format(
                                self.equipment,
                                self.query_parms[
                                    "end_date"][0]))
                    else:
                        self._psql_session.execute(
                            DETAILED_EXCHANGER_HEALTH_EH.format(
                                self.equipment, self.query_parms["start_date"][0], self.query_parms["end_date"][0]))

                    df_data = pd.DataFrame(self._psql_session.fetchall())

                    graph = []

                    if not df_data.empty:
                        df_data.sort_values("timestamp", ascending=True, inplace=True)
                        temp = {"y_axis": list(df_data["eh_ea_ua"]), "x_axis": list(df_data["timestamp"]),
                                FLAG_STATUS_VALUE: list(df_data[FLAG_STATUS_VALUE]),
                                "equipment_part": str(df_data["equipment_part"].iloc[0])}
                        graph.append(temp)

                    dict_data["graph_data"] = graph

                    if dict_data["data"] or dict_data["graph_data"]:
                        if dict_data["data"]:
                            dict_data["status"] = 0
                        else:
                            dict_data["status"] = 1
                    else:
                        dict_data = 1

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

            dict_data = self.compose_dict_object()

            self.get_color_coding_tabular(dict_data)
            self.get_fdhdr_tag(dict_data)
            self.get_equipment_health_data(dict_data)

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
def get_equipment_health_data(request, unit_name=None, console_name=None, equipment_name=None):
    """
    This function will return the Equipment Health value
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
            query_parms = json.loads(
                json.dumps(parse_qs(request.GET.urlencode())))
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = EquipmentHealth(query_parms, unit_name, console_name, equipment_name)
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
