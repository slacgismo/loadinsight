wget https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh && bash Anaconda3-2019.10-Linux-x86_64.sh -b && rm Anaconda3-2019.10-Linux-x86_64.sh

echo "PATH=~/anaconda3/bin:$PATH" > .bashrc

sudo apt-get update \
    && sudo apt-get install -y gcc libssl-dev nginx

curl -sL https://deb.nodesource.com/setup_13.x | sudo -E bash -
sudo apt-get install -y nodejs

sudo touch /etc/apt/sources.list.d/pgdg.list
sudo echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" | sudo tee -a /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
apt-get install -y postgresql-client-11 libpq-dev


source env.txt
export $(cut -d= -f1 env.txt)


# Change backend of matplotlib to Agg
vim /home/ubuntu/anaconda3/lib/python3.7/site-packages/matplotlib/mpl-data/matplotlibrc