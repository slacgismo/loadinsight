from django.contrib import admin
from django.conf.urls import include, url
from django.urls import re_path
from django.views.generic import TemplateView

urlpatterns = (
    url(r'^admin/', admin.site.urls),
    url(r'^auth/', include('djoser.urls.base')),
    url(r'^auth/', include('djoser.urls.authtoken')),
    url(r'^api/', include('backend.urls')),
    # keep the path below at bottom because it is used to catch all urls other than admin, auth and api
    re_path(r'^(?!(admin/|auth/|api/)).*$', TemplateView.as_view(template_name='index.html'), name='index')
)
