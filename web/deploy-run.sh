### Arg:
###    bash deploy-run.sh deployment_target
###    deployment_target:
###        web: deploy the website
###        task: deploy django-background-tasks
### 


#!/bin/bash
set -e

if [ "$1" == "" ]; then
	echo "Please enter the deployment target. Usage: bash deploy-run.sh deployment_target"
	exit 1
fi

# DO NOT USE WINDOWS \R\N in env.txt
# EXPORT ENV VARIABLES
source ~/loadinsight/web/env.txt
export $(cut -d= -f1 ~/loadinsight/web/env.txt)

# Activate conda env
source "${CONDA_PREFIX}/etc/profile.d/conda.sh" 
conda activate venv_loadinsight

# Check database status
psql $DATABASE_URL -c '\l' || (>&2 echo "Postgres is unavailable" && exit 1)
>&2 echo "Postgres is up - continuing"

pushd ~/loadinsight/web/backend/

if [ "x$DJANGO_MANAGEPY_MIGRATE" = 'xon' ]; then
	python manage.py migrate --noinput
fi

python manage.py collectstatic --noinput

# Start uwsgi processes
echo Starting uwsgi.

# Run uwsgi in nohup
if [ "$1" = "task" ]; then
	nohup uwsgi --ini ~/loadinsight/web/uwsgi_process_tasks.ini 2>&1 &
else		
	pushd ~/loadinsight/web/frontend/app/

	# Build front end 
	npm install .
	npm run build 

	popd

	# Check if the symbol link exists
	test -h /etc/nginx/sites-enabled/nginx.conf && sudo unlink /etc/nginx/sites-enabled/nginx.conf

	sudo ln -s ~/loadinsight/web/nginx.conf /etc/nginx/sites-enabled/
	sudo /etc/init.d/nginx restart

	nohup uwsgi --ini ~/loadinsight/web/uwsgi.ini 2>&1 &
fi
