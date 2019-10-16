# Project Setup

## Install Django and Django REST framework
```
pip install django
pip install djangorestframework
```

## Install JWT
```
pip install djangorestframework-jwt
```
You may also need to install django-rest-auth and django-allauth :
```
pip install django-rest-auth 
pip install django-allauth
```

## Install psycopg2
```
pip install psycopg2
```

## Start the project
### Migrate before run the project
```
python manage.py migrate
```
### Create a super user
```
python manage.py createsuperuser
```
### Run server
```
python manage.py runserver
```
### Functionalities

- login: navigate to http://localhost:8000/login/
- signup: curl -d '{"username":<user_name>, "password":<password>}' -H "Content-Type: application/json" -X POST http://127.0.0.1:8000/signup/
