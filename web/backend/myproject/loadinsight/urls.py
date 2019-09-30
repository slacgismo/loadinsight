from django.contrib import admin
from django.urls import path
import loadinsight.views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('login/',loadinsight.views.login)
]
