from django.contrib import admin
from django.urls import path
from django.views.generic import TemplateView
from rest_framework_jwt.views import obtain_jwt_token
from .views import *


urlpatterns = [
    path('', TemplateView.as_view(template_name='index.html'), name='index'),
    path('api/admin/', admin.site.urls),
    path('api/login/', obtain_jwt_token),
    path('api/current_user/', current_user),
    path('api/signup/', UserList.as_view()),
    path('api/registration/', sendEmail),
    path('execute/', execute_piplines),
    path('api/registration/confirm-email/', current_user, name='confirm')
]
