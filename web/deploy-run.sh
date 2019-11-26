### Arg:
###    bash deploy-run.sh deployment_target
###    deployment_target:
###        web: deploy the website
###        task: deploy django-background-tasks
### 


#!/bin/bash
set -e

# DO NOT USE WINDOWS \R\N in env.txt
source ~/loadinsight/web/env.txt
export $(cut -d= -f1 ~/loadinsight/web/env.txt)

source "${CONDA_PREFIX}/etc/profile.d/conda.sh" 
conda activate venv_loadinsight

pushd ~/loadinsight/web/frontend/app/

# Build front end 
npm install .
npm run build 

popd

# Check database status
psql $DATABASE_URL -c '\l' || (>&2 echo "Postgres is unavailable" && exit 1)

>&2 echo "Postgres is up - continuing"

pushd ~/loadinsight/web/backend/

if [ "x$DJANGO_MANAGEPY_MIGRATE" = 'xon' ]; then
	python manage.py migrate --noinput
fi

python manage.py collectstatic --noinput

popd

# Check if the symbol link exists
test -h /etc/nginx/sites-enabled/nginx.conf && sudo unlink /etc/nginx/sites-enabled/nginx.conf

sudo ln -s ~/loadinsight/web/nginx.conf /etc/nginx/sites-enabled/
sudo /etc/init.d/nginx restart

# Start uwsgi processes
echo Starting uwsgi.

# Run uwsgi in nohup
if [ "$1" != "" ]; then
    if [ "$1" = "task" ]; then
		exec uwsgi --ini ~/loadinsight/web/uwsgi_process_tasks.ini
	else
		exec uwsgi --ini ~/loadinsight/web/uwsgi.ini
	fi
else
    exec uwsgi --ini ~/loadinsight/web/uwsgi.ini
fi
