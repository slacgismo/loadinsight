from django.contrib.auth import get_user_model
from django.db import models


class Executions(models.Model):
    user_id = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    # TODO: there should be unique id to distinguish each execution itself, too. Try to refer to django uuid
    #  https://docs.djangoproject.com/en/1.8/ref/models/fields/#django.db.models.UUIDField
    algorithm = models.CharField(max_length=50)
    # status of executions
    create_time = models.DateTimeField(auto_now_add=True)
