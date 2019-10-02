from django.contrib import admin
from django.urls import path
from django.views.generic import TemplateView
from rest_framework_jwt.views import obtain_jwt_token
from loadinsight.views import current_user, UserList


urlpatterns = [
    path('', TemplateView.as_view(template_name='index.html'), name='index'),
    path('admin/', admin.site.urls),
    path('login/', obtain_jwt_token),
    path('current_user/', current_user),
    path('signup/', UserList.as_view())
]
