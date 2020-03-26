"""
This is the setting file for deployment

IMPORTANT: DO NOT TOUCH THIS FILE WHEN YOU ARE DOING LOCAL DEVELOPMENT

The main differences between this setting file with the original one are:
1. Different database settings. 
2. Different the email settings. 
3. Allowed_HOST
4. DEBUG was set to be False
"""

import os
from settings import * 

# -----------------------
import dj_database_url
# -----------------------

# -----------------------
# Debug should be set as 
# False in production mode
DEBUG = True
# -----------------------

# -----------------------
# The database url was configed in environment variables
# DATABASE_URL = postgres://USER:PASSWORD@HOST:PORT/NAME

DATABASES = {}
DATABASES['default'] = dj_database_url.config(conn_max_age=600)
# -----------------------


# -----------------------
# Add allowed hosts Please add ALLOWED_HOSTS accordingly 

ALLOWED_HOSTS = [os.environ['ALLOWED_HOSTS']]

# -----------------------


# -----------------------
# EMAIL SETTINGS
# Please put the following items into your ENV variables
# EMAIL: your email address
# EMAIL_HOST: your email host
# EMAIL_APP_PWD: your email's password or appkey
# 
# Again, PLEASE DO NOT LEAVE ANY SENSITIVE INFO REGARDING PASSWORD HERE
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
DEFAULT_FROM_EMAIL = os.environ['EMAIL']  # this should be exactly the same as EMAIL_HOST_USER
EMAIL_USE_TLS = True  # whether use TLS
EMAIL_HOST = os.environ['EMAIL_HOST']  # SMTP server.
EMAIL_PORT = 587  # port of SMTF server
EMAIL_HOST_USER = DEFAULT_FROM_EMAIL  # sender's email address
EMAIL_HOST_PASSWORD = os.environ['EMAIL_APP_PWD']  # password of sender's email address
EMAIL_FROM = EMAIL_HOST_USER
# -----------------------