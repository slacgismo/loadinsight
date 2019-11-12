from django.contrib.auth import get_user_model
from django.db import models


class Executions(models.Model):
    user_id = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    algorithm = models.CharField(max_length=50)
    # status of executions
    create_time = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return '_'.join([str(self.id), str(self.user_id.id)]) 
