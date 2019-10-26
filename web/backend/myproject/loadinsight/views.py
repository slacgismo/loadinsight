from django.contrib.auth.models import User
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from rest_framework import permissions, status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.views import APIView
from .serializers import UserSerializer, UserSerializerWithToken
from django.core.mail import send_mail
from rest_framework.permissions import IsAuthenticated
import sys
import logging
from load_model.execute_pipelines import init_error_reporting as  init_error_reporting
from load_model.execute_pipelines import execute_lctk as execute_lctk

@api_view(['GET'])
def current_user(request):
    """
    Determine the current user by their token, and return their data
    """
    serializer = UserSerializer(request.user)
    return Response(serializer.data)


class UserList(APIView):
    """
    Create a new user. It's called 'UserList' because normally we'd have a get
    method here too, for retrieving a list of all User objects.
    """

    permission_classes = (permissions.AllowAny,)
    queryset = User.objects.all()

    # create a new user
    # change the decorator to `ensure_csrf_cookie` in production
    @method_decorator(csrf_exempt)
    def post(self, request, format=None):
        serializer = UserSerializerWithToken(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            user.is_active = False
            email_body = """Thank you for the registration of LoadInsight! Please click the following link to activate your account: http://%s%s""" \
                %(request.get_host(), reverse('confirm', args=(serializer.data['username'], serializer.data['token'])).replace("\\", "/"))
            #   % (serializer.data['token'])
            send_mail(subject="subject",
                      message=email_body,
                      from_email="sunzhengyu01@gmail.com",
                      recipient_list=[serializer.data['email']]
                      )
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view()
def null_view(request):
    return Response(status=status.HTTP_400_BAD_REQUEST)


@api_view()
@permission_classes([IsAuthenticated])
def complete_view(request, username, token):
    user = UserSerializerWithToken(username = username, token = token)
    user.is_active = True
    return Response("Email account is activated")

@api_view(['GET'])
def execute_piplines(request):
    # permission_classes = (permissions.AllowAny,)
    try:
        # before we even attempt to run the pipeline the error reporting
        init_error_reporting()
        # start the pipeline
        execute_lctk()
    except Exception as exc:
        logging.exception(exc)
        sys.exit(exc)