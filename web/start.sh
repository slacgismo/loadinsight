#!/bin/bash
set -e


source /opt/conda/etc/profile.d/conda.sh
conda activate venv_loadinsight
yes | conda install uwsgi

# check database status
# until psql $DATABASE_URL -c '\l'; do
# 	>&2 echo "Postgres is unavailable - sleeping"
# 	sleep 1
# done

# >&2 echo "Postgres is up - continuing"

if [ "x$DJANGO_MANAGEPY_MIGRATE" = 'xon' ]; then
	python manage.py migrate --noinput
fi

python manage.py collectstatic --noinput

ln -s /usr/src/app/nginx.conf /etc/nginx/sites-enabled/
/etc/init.d/nginx start

# Start uwsgi processes
echo Starting uwsgi.

exec uwsgi --ini /usr/src/app/uwsgi.ini
