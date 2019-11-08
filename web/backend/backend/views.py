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
from backend.serializers import *
from backend.helpers import s3_helper
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk
import djoser.permissions
from background_task import background
from django.contrib.auth import get_user_model

import django.core.serializers

from django.core.serializers import deserialize
# from web.backend.backend.tests.serializers import ExecutionsSerializer

path = "./load_model/local_data/"

@background(schedule=timezone.now())
def execute_task(algorithm, user_id, execution_id, config_data):
    # before we even attempt to run the pipeline the error reporting
    init_error_reporting()
    # start the pipeline
    execute_lctk(algorithm, execution_id, config_data)
    user = get_user_model().objects.get(pk=user_id)
    user.email_user('Here is a notification', 'Your pipeline has been executed successfully!')


@api_view(['POST'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def run_pipeline(request, pipeline_name):
    config_data = request.data

    exe = Executions(user_id=request.user, algorithm=pipeline_name)
    exe.save()

    try:
        execute_task(pipeline_name, request.user.id, exe.id, config_data)
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


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_city(request, execution_id=None, result_dir=None, city_name=None):
    if execution_id is not None and result_dir is not None and city_name is not None:
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir + "/" + result_dir
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                    response_list = []
                    for image_name in image_list:
                        city = image_name.split("-")[1].split("_")[0]
                        if city == str(city_name):
                            response_list.append(image_name)
                    response = {"execution_result_by_city": response_list}
                    if len(response_list) != 0:
                        return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_state(request, execution_id=None, result_dir=None, state_name=None):
    if execution_id is not None and result_dir is not None and state_name is not None:
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir + "/" + result_dir
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                    response_list = []
                    for image_name in image_list:
                        state = image_name.split("-")[1].split("_")[1]
                        if state == str(state_name):
                            response_list.append(image_name)
                    response = {"execution_result_by_state": response_list}
                    if len(response_list) != 0:
                        return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_content(request, execution_id=None, result_dir=None, content_name=None):
    if execution_id is not None and result_dir is not None and content_name is not None:
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(path) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(path) if isdir(join(path, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = path + dir + "/" + result_dir
                    if os.path.exists(new_path) is False:
                        return Response(status=status.HTTP_404_NOT_FOUND)
                    image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                    response_list = []
                    for image_name in image_list:
                        content = image_name.split("-")[2].split(".")[0]
                        if content == str(content_name):
                            response_list.append(image_name)
                    response = {"execution_result_by_content": response_list}
                    if len(response_list) != 0:
                        return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)

@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_city_and_content(request, execution_id=None, result_dir=None, city_name=None,
                                       content_name=None):
    if execution_id is None or result_dir is None or city_name is None or content_name is None:
        return Response(status=status.HTTP_404_NOT_FOUND)
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        if os.path.exists(path) is False:
            return Response(status=status.HTTP_404_NOT_FOUND)
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return Response(status=status.HTTP_404_NOT_FOUND)
                image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                for image_name in image_list:
                    city = image_name.split("-")[1].split("_")[0]
                    content = image_name.split("-")[2].split(".")[0]
                    if city == str(city_name) and content == str(content_name):
                        new_path = path + dir + "/" + result_dir + "/" + image_name
                        with open(new_path, "rb") as fp:
                            file = fp.read()
                        response = HttpResponse(file, content_type='image/png')
                        response['Content-Disposition'] = "attachment; filename=" + image_name
                        return response
    return Response(status=status.HTTP_404_NOT_FOUND)



@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_result(request, execution_id=None, result_dir=None, city_name=None,
                          state_name=None, content_name=None):
    if os.path.exists(path) is False:
        return Response(status=status.HTTP_404_NOT_FOUND)
    if execution_id is not None and execution_id!='None':
        if result_dir is None or result_dir=='None':
            response_list = filter_executions_by_id(request, execution_id)
            if len(response_list) == 0:
                return Response(status=status.HTTP_404_NOT_FOUND)
            response = {"execution_dirs": response_list}
            return Response(response, status=status.HTTP_200_OK)
        else:
            if city_name is None or city_name=='None':
                response_list = filter_executions_by_result_dir(request, execution_id, result_dir)
                if len(response_list) == 0:
                    return Response(status=status.HTTP_404_NOT_FOUND)
                response = {"execution_result_dirs": response_list}
                return Response(response, status=status.HTTP_200_OK)
            else:
                if state_name is None or state_name == 'None':
                    if content_name is None or content_name =='None':
                        response_list = filter_executions_by_city(request, execution_id, result_dir, city_name)
                        response = {"execution_result_by_city": response_list}
                        if len(response_list) == 0:
                            return Response(status=status.HTTP_404_NOT_FOUND)
                        return Response(response, status=status.HTTP_200_OK)
                    filter_executions_by_city_and_content(request, execution_id, result_dir, city_name, content_name)
                else:
                    if content_name is None or content_name =='None':
                        response_list = filter_executions_by_state(request, execution_id, result_dir, state_name)
                        response = {"execution_result_by_state": response_list}
                        if len(response_list) == 0:
                            return Response(status=status.HTTP_404_NOT_FOUND)
                        return Response(response, status=status.HTTP_200_OK)
                    return filter_executions_by_city_and_content(request, execution_id, result_dir, city_name, content_name)
    return Response(status=status.HTTP_404_NOT_FOUND)


def filter_executions_by_id(request, execution_id=None):
    response_list = []
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir
                if os.path.exists(new_path) is False:
                    return response_list
                response_list = [f for f in listdir(new_path) if isdir(join(new_path, f))]
    return response_list


def filter_executions_by_result_dir(request, execution_id=None, result_dir=None):
    response_list = []
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return response_list
                response_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
    return response_list


def filter_executions_by_city(request, execution_id=None, result_dir=None, city_name=None):
    response_list=[]
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return response_list
                image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                for image_name in image_list:
                    city = image_name.split("-")[1].split("_")[0]
                    if city == str(city_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_state(request, execution_id=None, result_dir=None, state_name=None):
    response_list = []
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return response_list
                image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                for image_name in image_list:
                    state = image_name.split("-")[1].split("_")[1]
                    if state == str(state_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_content(request, execution_id=None, result_dir=None, content_name=None):
    response_list = []
    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return response_list
                image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                for image_name in image_list:
                    content = image_name.split("-")[2].split(".")[0]
                    if content == str(content_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_city_and_content(request, execution_id=None, result_dir=None, city_name=None,
                                       content_name=None):

    '''
    s3 ex:
    dir_name = 'loadinsight-bucket/a/'  # '<bucket-name>/dir/subdir/'
    for f in s3_helper.list_files_in_dir(dir_name):
        if not f.endswith('/'):
            file s3_helper.read_file_binary(dir_name + f)


    list_files_in_dir:
    Return: 
        1. list of file and dir: ['a.txt', 'a/', 'b/'], could be empty, dir ends with '/'
        2. None: dir input path not found

    read_file_binary:
    Return:
        1. binary content of file
        2. None: file not found
    
    '''

    executions = Executions.objects.filter(user_id=request.user)
    if executions:
        alldirs = [f for f in listdir(path) if isdir(join(path, f))]
        for dir in alldirs:
            if dir.startswith(execution_id):
                new_path = path + dir + "/" + result_dir
                if os.path.exists(new_path) is False:
                    return Response(status=status.HTTP_404_NOT_FOUND)
                image_list = [f for f in listdir(new_path) if isfile(join(new_path, f))]
                for image_name in image_list:
                    city = image_name.split("-")[1].split("_")[0]
                    content = image_name.split("-")[2].split(".")[0]
                    if city == str(city_name) and content == str(content_name):
                        new_path = path + dir + "/" + result_dir + "/" + image_name
                        with open(new_path, "rb") as fp:
                            file = fp.read()
                        response = HttpResponse(file, content_type='image/png')
                        response['Content-Disposition'] = "attachment; filename=" + image_name
                        return response
    return Response(status=status.HTTP_404_NOT_FOUND)