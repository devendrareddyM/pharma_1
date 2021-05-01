"""
File                :   user_routing 

Description         :   This file will route to the respective methods

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   15/04/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from UsersManagement import user_get, user_update, user_delete, user_get_all, user_create
from utilities.Constants import MESSAGE_KEY, GET_REQUEST, PUT_REQUEST, DELETE_REQUEST, POST_REQUEST
from utilities.api_response import HTTP_405_METHOD_NOT_ALLOWED


@csrf_exempt
def route_parms(request, username):
    """
    This method will handle the routing for the requested method and route parameters type to its
    functionality
    @:param request : request django object
    @:param username : requested user name
    """

    if request.method == GET_REQUEST:
        return user_get.get_user_list(request, username)
    elif request.method == PUT_REQUEST:
        return user_update.update_user(request)
    elif request.method == DELETE_REQUEST:
        return user_delete.delete_user(request, username)
    else:
        return JsonResponse({MESSAGE_KEY: "We haven't defined the more methods !"}, status=HTTP_405_METHOD_NOT_ALLOWED)


@csrf_exempt
def route(request):
    """
    This method will handle the routing for the requested method and route paramters type to its
    functionality
    @:param request : request django object
    """
    if request.method == GET_REQUEST:
        return user_get_all.get_all_user_list(request)
    elif request.method == POST_REQUEST:
        return user_create.create_user(request)
    else:
        return JsonResponse({MESSAGE_KEY: "We haven't defined the more methods !"}, status=HTTP_405_METHOD_NOT_ALLOWED)
