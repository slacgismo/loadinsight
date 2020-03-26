from rest_framework import serializers
from backend.models import Executions


class ExecutionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Executions
        fields = ('id', 'algorithm', 'create_time')
