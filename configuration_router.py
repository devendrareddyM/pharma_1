"""
File                :   configuration_router.py

Description         :   This will return the algorithm list and configuration update

Author              :   LivNSense Technologies

Date Created        :   11-11-2019

Date Last modified  :   11-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from configurations import algorithms_list, configuration_update
from utilities.Constants import GET_REQUEST, PUT_REQUEST, MESSAGE_KEY, METHOD_NOT_ALLOWED
from utilities.api_response import HTTP_405_METHOD_NOT_ALLOWED


@csrf_exempt
def configuration_with_params(request, algorithm_name):
    if request.method == GET_REQUEST:
        return algorithms_list.get_algorithm_list(request, algorithm_name)

    if request.method == PUT_REQUEST:
        return configuration_update.update_algorithm_params(request, algorithm_name)

    else:
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)


@csrf_exempt
def configuration_without_params(request):
    return algorithms_list.get_algorithm_list(request, None)
