from django.urls import path, re_path
from django.views.generic import TemplateView
from .views import *

urlpatterns = [
    path('api/executions/configs/', get_pipeline_configs),  # GET pipeline configs
    path('api/executions/', handle_executions),  # POST to execute, GET to get all executions
    # re_path(r'^api/executions/(?P<execution_id>\w+)/(?P<result_dir>\w+/(?P<city_name>\w+)/(?P<state_name>\w+)/'
    #         '(?P<content_name>\w+)', get_executions_result),  # GET execution info and results
    re_path(r'api/executions/(?P<execution_id>\w)/', get_result_dirs_of_execution),
    re_path(r'api/executions/(?P<execution_id>\w)/results/(?P<result_dir>\w+/', get_images_of_execution_result), # GET, but the GET body contains city name, state name, and content name
    # keep the path below at bottom because it is used to catch all other urls served by react
    re_path('', TemplateView.as_view(template_name='index.html'), name='index')
]


