# Project Setup

## Install dependencies
```bash
cd backend
pip install -r requirements.txt
```

## Start the project
### Migrate before run the project
```bash
python manage.py migrate
```

### Create a super user
```bash
python manage.py createsuperuser
```

### Run server
```bash
python manage.py runserver
```

### Running the frontend
```bash
cd frontend/app
npm install
npm run build
```

### Functionalities

- login: navigate to http://localhost:8000/login/
- signup: curl -d '{"username":<user_name>, "password":<password>}' -H "Content-Type: application/json" -X POST http://127.0.0.1:8000/signup/
