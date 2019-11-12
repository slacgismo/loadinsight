from os import listdir
from os.path import join, isdir
from django.http import HttpResponse
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from backend.serializers import *
from .helpers.s3_helper import *
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk
import djoser.permissions
from background_task import background
from background_task.models_completed import CompletedTask
from django.contrib.auth import get_user_model
from settings import S3_BUCKET_PATH, USER_CUSTOMIZABLE_CONFIGS


@background(schedule=timezone.now())
def execute_task(algorithm, user_id, execution_id, config_data):
    # before we even attempt to run the pipeline the error reporting
    init_error_reporting()
    # start the pipeline
    execute_lctk(algorithm, execution_id, config_data)
    user = get_user_model().objects.get(pk=user_id)
    user.email_user('Here is a notification', 'Your pipeline has been executed successfully!')


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_pipeline_configs(request):
    # get all config file in json data
    file_dir = os.path.dirname(os.path.realpath('__file__'))
    root_dir = os.path.join(file_dir, 'load_model/config/')
    config_json = {}
    for src_dir, dirs, files in os.walk(root_dir):
        for _file in files:
            file_name = os.path.join(src_dir, _file)
            if _file in USER_CUSTOMIZABLE_CONFIGS:
                with open(file_name, 'r') as json_file:
                    data = json.load(json_file)
                config_json[_file] = data

    return Response(config_json, status=status.HTTP_200_OK)


@api_view(['POST'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def execute_pipeline(request):
    config_data = request.data['configs']
    pipeline_name = config_data['pipeline_name']
    exe = Executions(user_id=request.user, algorithm=pipeline_name)
    exe.save()
    try:
        # creator is used to identify the owner of this task
        # verbose_name is used to associate the Execution objects 
        # with the database objects in Django-background-tasks (Tasks and Completed_Tasks)
        # IMPORTANT: DO NOT MODIFY THIS PART THANKS!
        execute_task(pipeline_name, request.user.id, exe.id, config_data, \
                                    creator = request.user, verbose_name = str(exe))
        return Response(status=status.HTTP_200_OK)
    except Exception as e:
        logging.exception(e)
        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def get_all_completed_exes(user_id, exe_id = None):
    """
        This function accept a user object and get all completed executions for this user 
        Args: 
            user_id: user object
            exe_id: str, a specific execution id, optional 
            TODO: need more discussions about whether we need support query completed executions with 
                  a execution id list. 
        Output:
            executions: a Execution queryset, all executions are completed. If exe_id is provided, will
                        Return a Execution queryset with this exe_id if this task has completed.
    """
    user = user_id

    completed_tasks = CompletedTask.objects.created_by(user).succeeded()

    exe_ids = [int(task.verbose_name.split('_')[0]) for task in completed_tasks]
    if exe_id is None:
        # Specific exe_id not provided 
        return Executions.objects.filter(pk__in=exe_ids)
    else:
        exe_id = int(exe_id)
        exe_ids = [exe_id] if exe_id in exe_ids else []
        return Executions.objects.filter(pk__in=exe_ids)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions(request):
    executions = get_all_completed_exes(user_id=request.user)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_executions_by_image(request, execution_id=None, result_dir=None, image_name=None):
    if execution_id is not None and result_dir is not None and image_name is not None:
        executions = Executions.objects.filter(user_id=request.user)
        if executions:
            if os.path.exists(S3_BUCKET_PATH) is False:
                return Response(status=status.HTTP_404_NOT_FOUND)
            alldirs = [f for f in listdir(S3_BUCKET_PATH) if isdir(join(S3_BUCKET_PATH, f))]
            for dir in alldirs:
                if dir.startswith(execution_id):
                    new_path = S3_BUCKET_PATH + dir + "/" + result_dir + "/" + image_name
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
def get_executions_result(request, execution_id=None, result_dir=None, city_name=None,
                          state_name=None, content_name=None):
    exe_id_flag = execution_id is None or execution_id =='None'
    result_dir_flag = result_dir is None or result_dir =='None'
    city_flag = city_name is  None or city_name =='None'
    state_flag = state_name is None or state_name =='None'
    content_flag = content_name is None or content_name =='None'

    if not exe_id_flag:
        if result_dir_flag:
            response_list = filter_executions_by_id(request, execution_id)
            response = {"execution_dirs": response_list}
        else:
            if city_flag and state_flag and content_flag:
                response_list = filter_executions_by_result_dir(request, execution_id, result_dir)
                response = {"execution_result_dirs": response_list}
            elif not city_flag and state_flag and content_flag:
                response_list = filter_executions_by_city(request, execution_id, result_dir, city_name)
                response = {"execution_result_by_city": response_list}
            elif city_flag and not state_flag and content_flag:
                response_list = filter_executions_by_state(request, execution_id, result_dir, state_name)
                response = {"execution_result_by_state": response_list}
            elif city_flag and state_flag and not content_flag:
                response_list = filter_executions_by_content(request, execution_id, result_dir, content_name)
                response = {"execution_result_by_content": response_list}
            elif not city_flag and not content_flag:
                return filter_executions_by_city_and_content(request, execution_id, result_dir,
                                                             city_name, content_name)
            elif not content_flag and not content_flag:
                return filter_executions_by_state_and_content(request, execution_id, result_dir,
                                                              state_name, content_name)
            else:
                response_list = filter_executions_by_city(request, execution_id, result_dir, city_name)
                response = {"execution_result_by_city": response_list}

        if len(response_list) == 0:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


def filter_executions_by_id(request, execution_id=None):
    response_list = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        alldirs = list_files_in_dir(S3_BUCKET_PATH)
        for dir in alldirs:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir
                dir_list = list_files_in_dir(new_path)
                if dir_list is None:
                    return response_list
                response_list = dir_list
    return response_list


def filter_executions_by_result_dir(request, execution_id=None, result_dir=None):
    response_list = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        alldirs = list_files_in_dir(S3_BUCKET_PATH)
        for dir in alldirs:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                print("new path:", new_path)
                dir_list = list_files_in_dir(new_path)
                if dir_list is None:
                    return response_list
                response_list = dir_list
    return response_list


def filter_executions_by_city(request, execution_id=None, result_dir=None, city_name=None):
    response_list=[]
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        alldirs = list_files_in_dir(S3_BUCKET_PATH)
        for dir in alldirs:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                image_list = list_files_in_dir(new_path)
                if image_list is None:
                    return response_list
                for image_name in image_list:
                    if algorithm == "ceus":
                        city = image_name.split("-")[1].split("_")[0]
                    else:
                        city = image_name.split("-")[-2].split("_")[0]
                    if city == str(city_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_state(request, execution_id=None, result_dir=None, state_name=None):
    response_list = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        alldirs = list_files_in_dir(S3_BUCKET_PATH)
        for dir in alldirs:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                image_list = list_files_in_dir(new_path)
                if image_list is None:
                    return response_list
                for image_name in image_list:
                    if algorithm == "ceus":
                        state = image_name.split("-")[1].split("_")[1]
                    else:
                        state = image_name.split("-")[-2].split("_")[1]
                    if state == str(state_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_content(request, execution_id=None, result_dir=None, content_name=None):
    response_list = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        alldirs = list_files_in_dir(S3_BUCKET_PATH)
        for dir in alldirs:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                image_list = list_files_in_dir(new_path)
                if image_list is None:
                    return response_list
                for image_name in image_list:
                    content = image_name.split("-")[-1].split(".")[0]
                    if content == str(content_name):
                        response_list.append(image_name)
    return response_list


def filter_executions_by_city_and_content(request, execution_id=None, result_dir=None, city_name=None,
                                       content_name=None):
    """
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
    """

    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        response_list = []
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        dir_list = list_files_in_dir(S3_BUCKET_PATH)
        for dir in dir_list:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                image_list = list_files_in_dir(new_path)
                if image_list is None:
                    return Response(status=status.HTTP_404_NOT_FOUND)
                image_list = filter_executions_by_city(request, execution_id, result_dir, city_name)
                for image_name in image_list:
                    content = image_name.split("-")[-1].split(".")[0]
                    if content == str(content_name):
                        response_list.append(image_name)
                if len(response_list) == 1:
                    new_path = S3_BUCKET_PATH + dir + result_dir + "/" + response_list[0]
                    file = read_file_binary(new_path)
                    response = HttpResponse(file, content_type='image/png')
                    response['Content-Disposition'] = "attachment; filename=" + response_list[0]
                    return response
                elif len(response_list) > 1:
                    response = {"execution_result_by_coty_and_content": response_list}
                    return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)


def filter_executions_by_state_and_content(request, execution_id=None, result_dir=None, state_name=None,
                                       content_name=None):

    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        response_list = []
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        dir_list = list_files_in_dir(S3_BUCKET_PATH)
        for dir in dir_list:
            if dir.startswith(execution_id) and dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + dir + result_dir
                image_list = list_files_in_dir(new_path)
                if image_list is None:
                    return Response(status=status.HTTP_404_NOT_FOUND)
                image_list = filter_executions_by_state(request, execution_id, result_dir, state_name)
                for image_name in image_list:
                    content = image_name.split("-")[-1].split(".")[0]
                    if content == str(content_name):
                        response_list.append(image_name)
                if len(response_list) ==1:
                    new_path = S3_BUCKET_PATH + dir + result_dir + "/" + response_list[0]
                    file = read_file_binary(new_path)
                    response = HttpResponse(file, content_type='image/png')
                    response['Content-Disposition'] = "attachment; filename=" + response_list[0]
                    return response
                elif len(response_list) >1:
                    response = {"execution_result_by_state_and_content": response_list}
                    return Response(response, status=status.HTTP_200_OK)
    return Response(status=status.HTTP_404_NOT_FOUND)