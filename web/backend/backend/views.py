import json
import os
import logging
from os import listdir
from os.path import isfile, join, isdir

from django.http import HttpResponse
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import permissions, status
from rest_framework.decorators import api_view, permission_classes

from backend.models import Executions
from backend.tests.serializers import *
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk
import djoser.permissions
from background_task import background
from django.contrib.auth import get_user_model

import django.core.serializers

from django.core.serializers import deserialize
# from web.backend.backend.tests.serializers import ExecutionsSerializer
from backend.tests.serializers import ExecutionsSerializer

path = "./load_model/local_data/"

@background(schedule=timezone.now())
def execute_task(algorithm, user_id, execution_id):
    # before we even attempt to run the pipeline the error reporting
    init_error_reporting()
    # start the pipeline
    execute_lctk(algorithm, execution_id)
    user = get_user_model().objects.get(pk=user_id)
    user.email_user('Here is a notification', 'Your pipeline has been executed successfully!')

@api_view(['GET'])
# TODO: Please note here why we use djoser.permissions.CurrentUserOrAdmin. It is because the auth is done via djoser.
#  https://djoser.readthedocs.io/en/latest/settings.html#permissions
#  Also, we shouldn't allow any people without auth to be able to execute the pipelines,
#  because it is dangerous and prone to excessive resource usage,
#  e.g, somebody run a script to execute 10000 times to attack your server.
#  You can have a try to replace this with rest_framework.permissions.IsAuthenticated ,
#  to understand the difference between these 2 permissions.
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def execute_piplines(request, algorithm):
    exe = Executions(user_id=request.user, algorithm=algorithm)
    exe.save()
    try:
        execute_task(algorithm, request.user.id, exe.id)
        return Response(status=status.HTTP_200_OK)
    except Exception as exc:
        logging.exception(exc)
        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# TODO: Please try to refrain from leaving unused code or comments in a pushed comment.
#  This can bring inconvenience in understanding the code.
#  You can postpone pushing the commit until you finish relevant part.
#  If there is necessity that you should push the unfinished part first (say, in case you lost them)
#  and you want to avoid create a temp separate branch,
#  please do add TO-DO comment or something obvious to note these part should be ignored by other readers.
@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions(request):
    #if execution_id is None:
        # get user's all executions as a list
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_id(request, execution_id=None):
    if execution_id is not None:
        response_list = []
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    response_list = [f for f in listdir(new_path) if isdir(join(new_path, f))]
            response = {"execution_dirs": response_list}
            return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_result_dir(request, execution_id=None, result_dir=None):
    if execution_id is not None and result_dir is not None:
        executions = Executions.objects.filter(user_id=request.user)
        print("enter:", executions)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir + "/" + result_dir
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    response_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                    response = {"execution_result_dirs": response_list}
                    return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_image(request, execution_id=None, result_dir=None, image_name=None):
    if execution_id is not None and result_dir is not None and image_name is not None:
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir + "/" + result_dir + "/" + image_name
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    with open(new_path, "rb") as fp:
                        file = fp.read()
                    response = HttpResponse(file, content_type='image/png')
                    response['Content-Disposition'] = "attachment; filename=" + image_name
                    return response
    return Response(status=status.HTTP_400_BAD_REQUEST)






