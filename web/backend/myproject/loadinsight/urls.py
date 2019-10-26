from django.contrib import admin
from django.urls import path, re_path
from django.views.generic import TemplateView
from rest_framework_jwt.views import obtain_jwt_token
from .views import *


urlpatterns = [
    path('api/admin/', admin.site.urls),
    path('api/login/', obtain_jwt_token),
    path('api/current_user/', current_user),
    path('api/signup/', UserList.as_view()),
    path('api/execute/', execute_piplines),
    path('api/registration/confirm-email/', current_user, name='account_confirm_complete'),
    path(r'api/^<username><token>', complete_view, name='confirm'),
    # keep the path below at bottom because it is used to catch all other urls served by react
    re_path('', TemplateView.as_view(template_name='index.html'), name='index')
]
