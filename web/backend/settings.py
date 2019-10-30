import os

DEBUG = True

BASE_DIR = os.path.dirname(__file__)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
    }
}

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
    "PASSWORD_RESET_CONFIRM_URL": "password/reset/confirm/{uid}/{token}",
    "USERNAME_RESET_CONFIRM_URL": "username/reset/confirm/{uid}/{token}",
    "ACTIVATION_URL": "activate/{uid}/{token}",
}

# PLEASE DO NOT LEAVE ANY SENSITIVE INFO REGARDING PASSWORD HERE
EMAIL_BACKEND = "django.core.mail.backends.console.EmailBackend"
DEFAULT_FROM_EMAIL = ''  # this should be exactly the same as EMAIL_HOST_USER
EMAIL_USE_TLS = True  # whether use TLS
EMAIL_HOST = 'smtp.mailtrap.io'  # SMTP server.
EMAIL_PORT = 2525  # port of SMTF server
EMAIL_HOST_USER = DEFAULT_FROM_EMAIL  # sender's email address
EMAIL_HOST_PASSWORD = ''  # password of sender's email address
EMAIL_FROM = EMAIL_HOST_USER

WSGI_APPLICATION = 'wsgi.application'

# settings for Django Background Task
MAX_ATTEMPTS = 1  # controls how many times a task will be attempted (default 25)
