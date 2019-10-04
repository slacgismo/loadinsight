import datetime

from rest_framework import serializers
from rest_framework_jwt.settings import api_settings
from django.contrib.auth.models import User
from .models import EmailVerifyCode

class UserSerializer(serializers.ModelSerializer):

    class Meta:
        model = User
        fields = ('username', 'email')


class UserSerializerWithToken(serializers.ModelSerializer):

    token = serializers.SerializerMethodField()
    password = serializers.CharField(write_only=True)

    def get_token(self, obj):
        jwt_payload_handler = api_settings.JWT_PAYLOAD_HANDLER
        jwt_encode_handler = api_settings.JWT_ENCODE_HANDLER

        payload = jwt_payload_handler(obj)
        token = jwt_encode_handler(payload)
        return token

    def create(self, validated_data):
        password = validated_data.pop('password', None)
        instance = self.Meta.model(**validated_data)
        if password is not None:
            instance.set_password(password)
        instance.save()
        return instance

    class Meta:
        model = User
        fields = ('token', 'username', 'password')


class VerifyCodeSerializer(serializers.Serializer):
    email = serializers.EmailField(max_length=50)

    def validate_email(self, email):
        if UserSerializer.filter(email=email):
            raise serializers.ValidationError('the user has already existed')
        # one_minute_age = datetime.now() - datetime.timedelta(minutes=1)
        # if EmailVerifyCode.objects.filter(add_time__gt=one_minute_age, email=email):
        #     raise serializers.ValidationError('still within 60s....')
        return email