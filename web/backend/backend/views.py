import sys
import logging
from rest_framework import permissions
from rest_framework.decorators import api_view, permission_classes
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk


@api_view(['GET'])
@permission_classes([permissions.AllowAny])
def execute_piplines(request):
    # permission_classes = (permissions.AllowAny,)
    try:
        # before we even attempt to run the pipeline the error reporting
        init_error_reporting()
        # start the pipeline
        execute_lctk()
    except Exception as exc:
        logging.exception(exc)
        sys.exit(exc)