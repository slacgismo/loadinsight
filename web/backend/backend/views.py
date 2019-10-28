import os
import sys
import logging

from rest_framework.response import Response
from rest_framework import permissions, status
from rest_framework.decorators import api_view, permission_classes

from backend.models import Executions
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
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
        sys.exit(exc)
        return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# @api_view(['GET'])
# @permission_classes([permissions.AllowAny])
# def get_images(request, timestamp, algorithm):
#     file_dir = os.path.dirname(os.path.realpath('__file__'))
#     dir = file_dir + '/load_model/load_data/' + timestamp + '__' + algorithm
#     if not os.path.exists(dir):
#         return Response(status=status.HTTP_400_BAD_REQUEST)




