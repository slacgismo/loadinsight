from rest_framework import serializers
from backend.models import *
from django.contrib.auth import get_user_model


class ExecutionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Executions
        fields = ('id', 'algorithm', 'create_time')
