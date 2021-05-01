"""
File                :   csv_download.py

Description         :   This will download csv/xml files for the selected algorithm

Author              :   LivNSense Technologies

Date Created        :   11-11-2019

Date Last modified  :   11-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import json
import os
import traceback
import zipfile

import jwt
import pandas as pd
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    GET_REQUEST, FILES_NAME_REQUEST, HTTP_AUTHORIZATION_TOKEN
from utilities.http_request import error_instance
from utilities.LoggerFile import log_error, log_debug
from Database.db_queries import SINGLE_FILE_DOWNLOAD_QUERY, MULTIPLE_FILES_DOWNLOAD_QUERY
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_403_FORBIDDEN, \
    HTTP_401_UNAUTHORIZED
from Database.Authentiction_tokenization import _CassandraConnection, _TokenValidation
from xml.dom.minidom import parseString
import dicttoxml
from Database.Configuration import NAME, TABLE_NAME


class DownloadAlgorithmFile(_CassandraConnection):
    """
    This class is responsible for downloading the csv files for the algorithm
    """

    def __init__(self, algo_name, query_params):
        """
        :param algo_name : this will have the algorithm name
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.algorithm_name = algo_name
        self.query_params = query_params

    """
    Download the zip file with csv/xml  
    """

    def download_file(self):
        """
        This will download all the zip files (contains xml/csv) for the selected algorithm.
        :return: zip file to download
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            files = self.query_params["files"].split(",")

            if len(files) == 1:
                query = SINGLE_FILE_DOWNLOAD_QUERY.format(NAME, TABLE_NAME, self.algorithm_name, files[0])
            else:
                query = MULTIPLE_FILES_DOWNLOAD_QUERY.format(NAME, TABLE_NAME, self.algorithm_name, tuple(files))

            result_set = self._csql_session.execute(query)
            result_set_list = list(result_set)

            if len(result_set_list) != len(files):
                return JsonResponse("Please enter the correct file names", safe=False)

            df_data = pd.DataFrame(result_set_list)

            """
            Creates Zip File
            """
            file_to_download = self.algorithm_name + ".zip"
            zip_object = zipfile.ZipFile(file_to_download, "w")

            """
            Iterate each file(row entry from DB) and creates csv, xml files inside zip file with the file name  
            """
            for index, row in df_data.iterrows():
                # df = pd.read_json(row['value'])
                extension = os.path.splitext(row['file_param_name'])[1]
                if extension == ".csv":
                    df = pd.read_json(row['value'])
                    zip_object.writestr(row['file_param_name'], df.to_csv(index=False))
                elif extension == ".xml":
                    # obj = json.loads(row['value'])
                    obj = row['value']
                    print(obj)
                    # xml = dicttoxml.dicttoxml(obj, root=False, attr_type=False)
                    # xml.partition(b'<?xml version="1.0" encoding="UTF-8" ?>')  # added the xml version
                    # dom = parseString(xml)
                    zip_object.writestr(row['file_param_name'],obj)
            zip_object.close()

            """Download Created Zip File"""

            with open(file_to_download, 'rb') as fh:
                response = HttpResponse(fh.read(), content_type="application/zip")
                response['Content-Disposition'] = 'attachment; file_name=' + os.path.basename(file_to_download)
                return response

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def download_algorithm_file(request, algorithm_name):
    """
    This function will download csv, xml files and will return error if generated.
    :param request: request django object
    :param algorithm_name : this will have the algorithm name
    :return: json response
    """
    query_params = obj = None
    try:

        query_params = {
            FILES_NAME_REQUEST: request.GET[FILES_NAME_REQUEST]
        }

    except:
        pass
    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = DownloadAlgorithmFile(algorithm_name, query_params)
                return obj.download_file()
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
