from django.urls import path, re_path
from django.views.generic import TemplateView


urlpatterns = [
    # keep the path below at bottom because it is used to catch all other urls served by react
    re_path('', TemplateView.as_view(template_name='index.html'), name='index')
]
