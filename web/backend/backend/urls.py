from django.urls import path, re_path
from django.views.generic import TemplateView
from .views import *
from django.urls import path

urlpatterns = [
    path('api/execute/<str:algorithm>/', execute_piplines),
    path('api/my_executions/', get_executions),
    path('api/my_executions/<str:execution_id>/', get_executions_by_id),
    path('api/my_executions/<str:execution_id>/results/<str:result_dir>/', get_executions_by_result_dir),
    path('api/my_executions/<str:execution_id>/results/<str:result_dir>/images/<str:image_name>/', get_executions_by_image),
    # keep the path below at bottom because it is used to catch all other urls served by react
    re_path('', TemplateView.as_view(template_name='index.html'), name='index')
]
