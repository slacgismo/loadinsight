from django.urls import path, re_path
from .views import *

urlpatterns = [
    path('executions/configs/', get_pipeline_configs),  # GET pipeline configs
    path('executions/', handle_executions),  # POST to execute, GET to get all executions
    re_path(r'^executions/(?P<execution_id>\w+)/$', get_result_dirs_of_execution),
    re_path(r'^executions/(?P<execution_id>\w+)/results/(?P<result_dir>\w+)/image/(?P<image_name>\w+)/$',
            get_executions_by_image),
    re_path(r'^executions/(?P<execution_id>\w+)/results/(?P<result_dir>\w+)/$', get_images_of_execution_result), # GET, but the GET body contains city name, state name, and content name
]


