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

MIDDLEWARE = ["django.contrib.sessions.middleware.SessionMiddleware"]


INSTALLED_APPS = (
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
    "templated_mail",
    "rest_framework",
    "rest_framework.authtoken",
    "djoser",
    "backend",
)

BUILD_DIR = os.path.join(BASE_DIR, '../../frontend/app/build')

STATICFILES_DIRS = [
    os.path.join(BUILD_DIR, 'static'),
]

STATIC_URL = "/static/"

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework.authentication.TokenAuthentication",
    ),
}

ROOT_URLCONF = "urls"

TEMPLATES = [
    {"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True}
]

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
]


DJOSER = {
    "SEND_ACTIVATION_EMAIL": True,
    "PASSWORD_RESET_CONFIRM_URL": "#/password/reset/confirm/{uid}/{token}",
    "USERNAME_RESET_CONFIRM_URL": "#/username/reset/confirm/{uid}/{token}",
    "ACTIVATION_URL": "#/activate/{uid}/{token}",
}

EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
DEFAULT_FROM_EMAIL = '550de45ffd017c'  # this should be exactly the same as EMAIL_HOST_USER
EMAIL_USE_TLS = True   #whether use TLS
EMAIL_HOST = 'smtp.mailtrap.io'   #SMTP server
EMAIL_PORT = 2525     #port of SMTF server
EMAIL_HOST_USER = DEFAULT_FROM_EMAIL    #sender's email address
EMAIL_HOST_PASSWORD = 'de1e687d7c0ddb'         #password of sender's email address
EMAIL_FROM = EMAIL_HOST_USER


WSGI_APPLICATION = 'wsgi.application'

