from os import listdir
from os.path import join, isdir
from django.http import HttpResponse
from django.utils import timezone
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from backend.serializers import *
from .helpers.s3_helper import *
from load_model.execute_pipelines import init_error_reporting as init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk
import djoser.permissions
from background_task import background
from background_task.models_completed import CompletedTask
from django.contrib.auth import get_user_model
from settings import S3_BUCKET_PATH, USER_CUSTOMIZABLE_CONFIGS, EMAIL_RETRY_TIMES


@background(schedule=timezone.now())
def execute_task(algorithm, user_id, execution_id, config_data):
    # before we even attempt to run the pipeline the error reporting
    init_error_reporting()
    # start the pipeline
    execute_lctk(algorithm, execution_id, config_data)
    user = get_user_model().objects.get(pk=user_id)
    exe = Executions.objects.get(pk=execution_id)
    # Retry email send EMAIL_RETRY_TIMES times 
    for retry in range(EMAIL_RETRY_TIMES):
        try:
            user.email_user('Here is a notification for your execution',
                            'Your pipeline {0}, started at {1} has been executed successfully!'.format(algorithm, str(exe.create_time)))
        except:
            logging.exception("Send email to {0} failed | retried {1} times ".format(user_id, retry))
            continue
        break


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


@api_view(['POST', 'GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def handle_executions(request):
    if request.method == "POST":
        config_data = request.data['configs']
        pipeline_name = request.data['pipeline_name']
        exe = Executions(user_id=request.user, algorithm=pipeline_name)
        exe.save()
        try:
            #  creator is used to identify the owner of this task
            #  verbose_name is used to associate the Execution objects 
            #  with the database objects in Django-background-tasks (Tasks and Completed_Tasks)
            # IMPORTANT: DO NOT MODIFY THIS PART THANKS!
            execute_task(pipeline_name, request.user.id, exe.id, config_data,
                         creator=request.user, verbose_name=str(exe))
            return Response(status=status.HTTP_200_OK)
        except Exception as e:
            logging.exception(e)
            return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    elif request.method == "GET":
        executions = get_executions_with_status(user_id=request.user)
        if executions:
            return Response(executions, status=status.HTTP_200_OK)
        return Response(status=status.HTTP_404_NOT_FOUND)


def get_executions_with_status(user_id):
    """
        This function accept a user object and get all executions with status for this user 
        Args: 
            user_id: user object
        Output:
            a list of executions:
                executions: a Execution queryset, all executions are completed.
            Example: 
            {
                "id": 16,
                "algorithm": "rbsa",
                "create_time": "2019-11-11T16:07:25.307645",
                "status": f/s/r,
            }
    """
    user = user_id

    succeeded_tasks = CompletedTask.objects.created_by(user).succeeded()
    failed_tasks = CompletedTask.objects.created_by(user).failed()

    succeeded_exe_ids = set([int(task.verbose_name.split('_')[0]) for task in succeeded_tasks])
    failed_exe_ids = set([int(task.verbose_name.split('_')[0]) for task in failed_tasks])

    exes = Executions.objects.filter(user_id=user)

    res = []
    for exe in exes:
        status = ''
        if exe.id in succeeded_exe_ids:
            status = "succeeded"
        elif exe.id in failed_exe_ids:
            status = "failed"
        else:
            status = "running"

        res.append({
            "id": exe.id,
            "algorithm": exe.algorithm,
            "time": str(exe.create_time),
            "status": status
        })
    return res


def get_all_completed_exes(user_id, exe_id=None):
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
def get_executions_by_image(request, execution_id=None, result_dir=None, image_name=None):
    if execution_id is not None and result_dir is not None and image_name is not None:
        executions = get_all_completed_exes(request.user, execution_id)
        if executions:
            serializer = ExecutionsSerializer(executions, many=True)
            data = json.loads(json.dumps(serializer.data))
            algorithm = data[0]['algorithm']
            all_dirs = list_files_in_dir(S3_BUCKET_PATH)
            for _dir in all_dirs:
                if _dir.startswith(execution_id) and _dir.__contains__(algorithm):
                    new_path = S3_BUCKET_PATH + _dir + result_dir + "/" + image_name
                    image = read_file_binary(new_path)
                    response = HttpResponse(image, content_type='image/png')
                    response['Content-Disposition'] = "attachment; filename=" + image_name
                    return response
    return Response(status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_result_dirs_of_execution(request, execution_id):
    """
        Get the result dirs of an execution given exe id
        Args: 
            exe_id: execution id 
        Output:
            execution_dirs: the result dirs of this execution
    """
    # Check if exe id is an integer
    try:
        exeid = int(execution_id)
    except:
        return Response(status=status.HTTP_404_NOT_FOUND)
    response_list = filter_executions_by_id(request, execution_id)
    response = {"execution_dirs": response_list}
    if len(response_list) == 0:
        return Response(status=status.HTTP_404_NOT_FOUND)
    return Response(response, status=status.HTTP_200_OK)

@api_view(['GET'])
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def get_images_of_execution_result(request, execution_id, result_dir):
    """
        Get the images of an execution given parameters
        Args: 
            exe_id: execution id 
            result_dir: dirs of this execution
            request.data['city_name']: city filter 
            request.data['state_name']: state filter 
            request.data['content_name']: content filter 
        Output:
            execution_dirs: the result dirs of this execution
    """
    # Check if exe id is an integer
    try:
        exeid = int(execution_id)
    except:
        return Response(status=status.HTTP_404_NOT_FOUND)
    city_name_filter = request.GET.get("city_name", None)
    state_name_filter = request.GET.get('state_name', None)
    content_name_filter = request.GET.get('content_name', None)

    images = filter_executions_by_result_dir(request, execution_id, result_dir)

    response_list = []
    for image in images:
        image_name = image["image_name"]
        state_name = image["state_name"]
        city_name = image["city_name"]
        content_name = image["content_name"]
        if city_name_filter is None or city_name_filter.lower() == city_name.lower():
            if state_name_filter is None or state_name_filter.lower() == state_name.lower():
                if content_name_filter is None or content_name_filter.lower() == content_name.lower():
                    response_list.append(image_name)
    response = {
        "images": response_list
    }
    if len(response_list) == 0:
        return Response(status=status.HTTP_404_NOT_FOUND)
    return Response(response, status=status.HTTP_200_OK)


def filter_executions_by_id(request, execution_id=None):
    response_list = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        all_dirs = list_files_in_dir(S3_BUCKET_PATH)
        for _dir in all_dirs:
            if _dir.startswith(execution_id) and _dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + _dir
                dir_list = list_files_in_dir(new_path)
                if dir_list is None:
                    return response_list
                response_list = dir_list
    return response_list


def filter_executions_by_result_dir(request, execution_id=None, result_dir=None):
    """
        Get the list of images and coresponding state_name, city_name, and content_name
        Args: 
            exe_id: execution id 
            result_dir: dirs of this execution
        Output:
            [
                {
                    image_name: xxx
                    state_name: xxx
                    city_name: xxx
                    content_name: xxx 
                }
                ...
            ]
    """
    response_list = []
    res = []
    executions = get_all_completed_exes(request.user, execution_id)
    if executions:
        serializer = ExecutionsSerializer(executions, many=True)
        data = json.loads(json.dumps(serializer.data))
        algorithm = data[0]['algorithm']
        all_dirs = list_files_in_dir(S3_BUCKET_PATH)
        for _dir in all_dirs:
            if _dir.startswith(execution_id) and _dir.__contains__(algorithm):
                new_path = S3_BUCKET_PATH + _dir + result_dir
                print("new path:", new_path)
                dir_list = list_files_in_dir(new_path)
                if dir_list is None:
                    return response_list
                response_list = dir_list
        
        for image_name in response_list:
            city_name = ""
            state_name = ""
            content_name = ""
            if result_dir == "loadshapes" or result_dir == "ceus_loadshapes":
                city_name = state_name = content_name = None
            else:
                if algorithm == "ceus":
                    city_name = image_name.split("-")[1].split("_")[0]
                    state_name = image_name.split("-")[1].split("_")[1]
                else:
                    city_name = image_name.split("-")[-2].split("_")[0]
                    state_name = image_name.split("-")[-2].split("_")[1]        
                content_name = image_name.split("-")[-1].split(".")[0]

            res.append(
                {
                    "image_name": image_name,
                    "state_name": state_name,
                    "city_name": city_name,
                    "content_name": content_name
                }
            )
    return res



