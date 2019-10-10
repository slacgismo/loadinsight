import datetime

from django.db import models
from django.contrib.auth.models import User

# Create your models here.

class EmailVerifyCode(models.Model):
    SEND_TYPE = (
        ('register', 'register'),
        ('forget', ',forget password'),
    )
    code = models.CharField(max_length=6, verbose_name='verifyCode')
    email = models.EmailField(max_length=50, verbose_name='email')
    send_type = models.CharField(max_length=10, choices=SEND_TYPE, verbose_name='sendType')
    add_time = models.DateTimeField(verbose_name='addTime', default=datetime.datetime.now())

    def __str__(self):
        return self.code

    class Meta:
        verbose_name = 'verifyCode'
        verbose_name_plural = verbose_name

