from django.contrib import admin
from django.urls import path
from rest_framework_jwt.views import obtain_jwt_token
from loadinsight.views import current_user, UserList

urlpatterns = [
    path('admin/', admin.site.urls),
    path('login/', obtain_jwt_token),
    path('current_user/', current_user),
    path('signup/', UserList.as_view())
]
