#!/bin/bash
set -e

until psql $DATABASE_URL -c '\l'; do
	>&2 echo "Postgres is unavailable - sleeping"
	sleep 1
done

>&2 echo "Postgres is up - continuing"

if [ "x$DJANGO_MANAGEPY_MIGRATE" = 'xon' ]; then
	python manage.py migrate --noinput
fi

python manage.py collectstatic --noinput

# Start Gunicorn processes
echo Starting Gunicorn.
exec gunicorn myproject.wsgi:application \
	    --bind 0.0.0.0:8000 \
	        --workers 3
