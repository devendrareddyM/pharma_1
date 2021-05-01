"""
File                :   plant_performance_tracker.py

Description         :   This will return the PPT value for the request parameters

Author              :   LivNSense Technologies

Date Created        :   26-08-2019

Date Last modified :    26-08-2019

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


class PlantPerformanceTracker(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and respond for the Plant performance data
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
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()
        tag_name = []
        tag_value = []
        dol = []
        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )

        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        performance_tracker_status = GREEN_TAG
        if self.console == HOT_CONSOLE_1_VALUE:
            dict_data = {
                TIMESTAMP_KEY: self.sync_time,
                "FDHDR_TAG": tag_name,
                "FDHDR_VALUE": tag_value,
                "DOL_VALUE": dol,
                "status": performance_tracker_status,
                "data": []
            }
            return dict_data
        else:
            dict_data = {
                TIMESTAMP_KEY: self.sync_time,
                "status": performance_tracker_status,
                "data": []
            }
            return dict_data

    def get_color_coding_tabular(self, dict_data):
        self._psql_session.execute("select timestamp from color_coding_graph limit 1 ")
        timetsamp_record = self._psql_session.fetchone()

        if not timetsamp_record:
            return JsonResponse("No data available right now ! We'll be sending the empty response soon! ", )

        self.sync_time = timetsamp_record[TIMESTAMP_KEY]
        """ === """
        """
        Color Coding for tabular data
        """

        COLOR_CODING_TABULAR = "select * from color_coding_tabular where equipment_name = '{}' and feature = '{}' "

        try:
            self._psql_session.execute(COLOR_CODING_TABULAR.format(self.equipment, PERFORMANCE_TRACKER))

            tabular_df = pd.DataFrame(self._psql_session.fetchall())

            if not tabular_df.empty:

                try:
                    dict_data['status'] = int(
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
                self._psql_session.execute(FDHDR_VALUE.format(i, epoch_time))
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

        if fdhr_value not in CHECK_FDHDR_VALUE and self.console == HOT_CONSOLE_1_VALUE:
            dict_data["DOL_VALUE"] = None
            dict_data["status"] = 0
            dict_data["data"] = []

    def get_plant_performance_trakcer_data(self, dict_data):
        if self.console == COLD_CONSOLE_2_VALUE:

            if self.equipment == 'DA-401':
                self._psql_session.execute(DETAILED_DE_ETHANIZER_PPT_QUERY.format(self.sync_time, self.equipment))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data["data"] = yaml.safe_load(df.to_json(orient=RECORDS))
            else:
                self._psql_session.execute(
                    DETAILED_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment, self.sync_time))
                df = pd.DataFrame(self._psql_session.fetchall())
                dict_data["data"] = yaml.safe_load(df.to_json(orient=RECORDS))
            if df.empty:
                dict_data["status"] = 0
        elif self.console == HOT_CONSOLE_1_VALUE or self.console == HOT_CONSOLE_2_VALUE or self.console == COLD_CONSOLE_1_VALUE:
            self._psql_session.execute(
                DETAILED_FURNACE_PERFORMANCE_TRACKER_QUERY.format(self.equipment, self.sync_time))
            df = pd.DataFrame(self._psql_session.fetchall())
            dict_data["data"] = yaml.safe_load(df.to_json(orient=RECORDS))
            if df.empty:
                dict_data["status"] = 0

    def get_values(self):
        """
        This will return the data on the bases of the unit , equipment and console name for
        the deviation tracker values from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            # todo : respond back the empty json reponse
            dict_data = self.compose_dict_object()
            self.get_color_coding_tabular(dict_data)
            self.get_plant_performance_trakcer_data(dict_data)
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
def get_equipment_plant_performance_tracker_data(request, unit_name=None, console_name=None,
                                                 equipment_name=None):  # *args, **kwargs
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

        query_params = {"start_date": request.GET["start_date"],
                        "end_date": request.GET["end_date"]}
    except:
        pass

    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = PlantPerformanceTracker(query_params, unit_name, console_name, equipment_name)
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
