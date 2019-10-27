from django.conf.urls import include, url
from django.urls import path

urlpatterns = (
    url(r"^auth/", include("djoser.urls.base")),
    url(r"^auth/", include("djoser.urls.authtoken")),
    # keep the path below at bottom because it is used to catch all urls other than auth
    path('', include('backend.urls')),
)
