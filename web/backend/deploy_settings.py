"""
This is the setting file for deployment

The main differences between this setting file with the original one are:
1. Different database settings. 
2. Different the email settings. 
"""

import os

# -----------------------
import dj_database_url
# -----------------------

DEBUG = True

BASE_DIR = os.path.dirname(__file__)

# -----------------------
# The database url was configed in environment variables
# DATABASE_URL = postgres://USER:PASSWORD@HOST:PORT/NAME

DATABASE = {}
DATABASES['default'] = dj_database_url.config(conn_max_age=600)
# -----------------------


AUTH_PASSWORD_VALIDATORS = [{"NAME": "backend.validators.Is666"}]

SECRET_KEY = "@=dd@hf(quaim(*xu1f%g8&1ig0lnrg8-_w3^ho89705cc+pw6"

MIDDLEWARE = ["django.contrib.sessions.middleware.SessionMiddleware",
              'django.contrib.auth.middleware.AuthenticationMiddleware',
              'django.contrib.messages.middleware.MessageMiddleware',
              ]

INSTALLED_APPS = (
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    'django.contrib.messages',
    "django.contrib.staticfiles",
    "templated_mail",
    "rest_framework",
    "rest_framework.authtoken",
    "djoser",
    "backend",
    'background_task',
)

BUILD_DIR = os.path.join(BASE_DIR, '../frontend/app/build')

STATICFILES_DIRS = [
    os.path.join(BUILD_DIR, 'static'),
]

STATIC_URL = "/static/"

STATIC_ROOT = os.path.join(BASE_DIR, '../static')

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework.authentication.TokenAuthentication",
    ),
}

ROOT_URLCONF = "urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BUILD_DIR],
        "APP_DIRS": True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    }
]

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
]

DJOSER = {
    "SEND_ACTIVATION_EMAIL": True,
    "PASSWORD_RESET_CONFIRM_URL": "/password/reset/confirm/{uid}/{token}",
    "USERNAME_RESET_CONFIRM_URL": "/username/reset/confirm/{uid}/{token}",
    "ACTIVATION_URL": "/activate/{uid}/{token}",
}

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


WSGI_APPLICATION = 'wsgi.application'

# settings for Django Background Task
MAX_ATTEMPTS = 1  # controls how many times a task will be attempted (default 25)
