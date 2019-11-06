from django.conf.urls import url
from django.urls import path, re_path
from django.views.generic import TemplateView
from .views import *
from django.urls import path

urlpatterns = [
    path('api/run_pipeline/<str:pipeline_name>/', run_pipeline),
    path('api/my_executions/', get_executions),
    # path('api/my_executions/<str:execution_id>/', get_executions_by_id),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/', get_executions_by_result_dir),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/images/<str:image_name>/',
    #      get_executions_by_image),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/city/<str:city_name>/',
    #      get_executions_by_city),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/state/<str:state_name>/',
    #      get_executions_by_state),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/content/<str:content_name>/',
    #      get_executions_by_content),
    # path('api/my_executions/<str:execution_id>/results/<str:result_dir>/city/<str:city_name>/content/<str:content_name>/',
    #      get_executions_by_city_and_content),

    re_path('^api/my_executions/(?P<execution_id>\w+)/(?P<result_dir>\w+)/(?P<city_name>\w+)/(?P<state_name>\w+)/'
        '(?P<content_name>\w+)', get_executions_result),
    # keep the path below at bottom because it is used to catch all other urls served by react
    re_path('', TemplateView.as_view(template_name='index.html'), name='index')
]


