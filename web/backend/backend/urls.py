from django.urls import path, re_path
from .views import *

urlpatterns = [
    path('api/executions/configs/', get_pipeline_configs),  # GET pipeline configs
    path('api/executions/', handle_executions),  # POST to execute, GET to get all executions
    re_path(r'api/executions/(?P<execution_id>\w)/', handle_executions),
    re_path(r'api/executions/(?P<execution_id>\w)/results/(?P<result_dir>\w+)/', handle_executions), # GET, but the GET body contains city name, state name, and content name
]


