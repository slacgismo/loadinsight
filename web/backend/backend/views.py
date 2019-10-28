import os
import sys
import logging

from rest_framework.response import Response
from rest_framework import permissions, status
from rest_framework.decorators import api_view, permission_classes

from backend.models import Executions
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk
import djoser.permissions


@api_view(['GET'])
# TODO: Please note here why we use djoser.permissions.CurrentUserOrAdmin. It is because the auth is done via djoser.
#  https://djoser.readthedocs.io/en/latest/settings.html#permissions
#  Also, we shouldn't allow any people to be able to execute the pipelines,
#  because it is dangerous and prone to excessive resource usage,
#  e.g, somebody run a script to execute 10000 times to attack your server.
#  You can have a try to replace this with rest_framework.permissions.IsAuthenticated ,
#  to understand the difference between these 2 permissions.
@permission_classes([djoser.permissions.CurrentUserOrAdmin])
def execute_piplines(request, algorithm):
    exe = Executions(user_id=request.user, algorithm=algorithm)
    exe.save()
    try:
        # before we even attempt to run the pipeline the error reporting
        init_error_reporting()
        # start the pipeline
        execute_lctk(algorithm)
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
# @api_view(['GET'])
# @permission_classes([permissions.AllowAny])
# def get_images(request, timestamp, algorithm):
#     file_dir = os.path.dirname(os.path.realpath('__file__'))
#     dir = file_dir + '/load_model/load_data/' + timestamp + '__' + algorithm
#     if not os.path.exists(dir):
#         return Response(status=status.HTTP_400_BAD_REQUEST)




