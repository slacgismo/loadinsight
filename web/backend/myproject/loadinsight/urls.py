from django.contrib import admin
from django.urls import path
from django.views.generic import TemplateView
from rest_framework_jwt.views import obtain_jwt_token
from .views import *
from . import views
from django.conf.urls import url, include
from allauth.account.views import ConfirmEmailView





urlpatterns = [
    path('', TemplateView.as_view(template_name='index.html'), name='index'),
    path('admin/', admin.site.urls),
    path('login/', obtain_jwt_token),
    path('current_user/', current_user),
    path('signup/', UserList.as_view()),
    path('registration/', sendEmail),
    path(r'^registration/confirm-email/$', current_user, name='confirm'),
    # # Override urls
    # url(r'^registration/account-email-verification-sent/', views.null_view, name='account_email_verification_sent'),
    # url(r'^registration/account-confirm-email/(?P<key>[-:\w]+)/$', ConfirmEmailView.as_view(),name='account_confirm_email'),
    # url(r'^registration/complete/$', views.complete_view, name='account_confirm_complete'),
    # url(r'^password-reset/confirm/(?P<uidb64>[0-9A-Za-z_\-]+)/(?P<token>[0-9A-Za-z]{1,13}-[0-9A-Za-z]{1,20})/$',
    #     views.null_view, name='password_reset_confirm'),
    # # Default urls
    url(r'', include('rest_auth.urls')),
    # url(r'^registration/', include('rest_auth.registration.urls'))
]
