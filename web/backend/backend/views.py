import os
import sys
import logging

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


@background(schedule=timezone.now())
def execute_task(algorithm, user_id):
    # before we even attempt to run the pipeline the error reporting
    init_error_reporting()
    # start the pipeline
    execute_lctk(algorithm, user_id)
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
        execute_task(algorithm, request.user.id)
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
def get_executions(request, execution_id=None, result_dir=None, image_name=None):
    if execution_id is None:
        # get user's all executions as a list
        executions = Executions.objects.filter(user_id=request.user)
        serializer = ExecutionsSerializer(executions, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    elif result_dir is None:
        # TODO: get one execution's all directories name as a list
        execution = Executions.objects.get(id=int(execution_id))
    elif image_name is None:
        # TODO: get all images name in one directory of an execution as a list
        pass
    else:
        # TODO: get an image by name
        pass





