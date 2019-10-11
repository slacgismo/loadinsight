from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import permissions, status, viewsets, mixins
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.views import APIView
from .serializers import UserSerializer, UserSerializerWithToken, VerifyCodeSerializer
from random import choice
from django.core.mail import send_mail
from .models import EmailVerifyCode



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
    def post(self, request, format=None):
        serializer = UserSerializerWithToken(data=request.data)
        code = generate_code()
        if serializer.is_valid():
            user = serializer.save()
            user.is_active = False
            email_body = """Thank you for the registration of LoadInsight! Please click the following link to activate your account: http://registration/confirm-email/%s""" \
                         % (serializer.data['token'])

            send_mail(subject="subject",
                      message=email_body,
                      from_email="webzhengyus@163.com",
                      recipient_list=[serializer.data['email']]
                      )
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view()
def null_view(request):
    return Response(status=status.HTTP_400_BAD_REQUEST)


@api_view()
def complete_view(request):
    return Response("Email account is activated")


def generate_code():
    seeds = '1234567890qwertyuiopasdfghjklzxcvbnm'
    random_str = []
    for i in range(6):
        random_str.append(choice(seeds))
    return ''.join(random_str)


@api_view(['GET', 'POST'])
def sendEmail(request):
    if request.method == 'GET':
        return Response(request.data, status=status.HTTP_201_CREATED)

    elif request.method == 'POST':
        email = request.data
        code = generate_code()
        send_mail(subject="Verify your email address",
                  message="Your code for verification is: " + code,
                  from_email="webzhengyus@163.com",
                  recipient_list=["webzhengyus@163.com"]
                  )
        # send_status = send_mail(code, [email])
        # code = send_email(email, 'register')
        print("good request")
        return Response({
            'email': email,
            'code': code,
        }, status=status.HTTP_201_CREATED)