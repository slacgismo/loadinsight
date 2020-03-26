sudo apt-get update && sudo apt-get install -y gcc libssl-dev 

CONDAENV='off'
# Install Coressponding 
npm --version > /dev/null 2>&1 || NPM='on'
psql --version > /dev/null 2>&1 || PSQL='on'
nginx -v > /dev/null 2>&1 || NGINX='on'
conda list -n venv_loadinsight > /dev/null 2>&1 || CONDAENV='on'


if [ "$NPM" = 'on' ]; then
    echo "Install Nodejs"
	curl -sL https://deb.nodesource.com/setup_13.x | sudo -E bash -
    sudo apt-get install -y nodejs
fi

if [ "$PSQL" = 'on' ]; then
    echo "Install Psql"
	sudo touch /etc/apt/sources.list.d/pgdg.list
    sudo echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" | sudo tee -a /etc/apt/sources.list.d/pgdg.list
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update && sudo apt-get install -y postgresql-client-11 libpq-dev
fi

if [ "$NGINX" = 'on' ]; then
    echo "Install Nginx"
	sudo apt-get install -y nginx
fi


if [ "$CONDAENV" = 'off' ]; then
    conda env update --quiet -f ~/loadinsight/web/loadinsight-environment.yml
else
    conda config --add channels conda-forge > /dev/null 2>&1
    conda env create --quiet -f ~/loadinsight/web/loadinsight-environment.yml
    conda install -y -n venv_loadinsight uwsgi 
fi

source $(conda info --base)/etc/profile.d/conda.sh
conda activate venv_loadinsight

# Update the backend for matplotlib 
matplotfile=$(python -c "import matplotlib; print(matplotlib.matplotlib_fname())")
# -F   Interpret PATTERN as a list of fixed strings, separated by newlines, any of which is to be matched.
# -x Select only those matches that exactly match the whole 
# -q Quiet; do not write anything to standard output. Exit immediately with zero status if any match is found, even if an error was detected. Also see the -s or --no-messages option.
echo $matplotfile
! grep -Fxq "backend      : Agg" $matplotfile && (echo "backend      : Agg" | sudo tee -a $matplotfile)

# REPLACE %%HOME%% in config files with $HOME
search="%%HOME%%"
replace=$HOME
sed -i "s_${search}_${replace}_g" ~/loadinsight/web/uwsgi.ini
sed -i "s_${search}_${replace}_g" ~/loadinsight/web/nginx.conf 
