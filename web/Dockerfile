
# Dockerfile

# FROM directive instructing base image to build upon
FROM continuumio/miniconda3

# COPY startup script into known file location in container
# COPY start.sh /usr/src/app/start.sh

# EXPOSE port 8000 to allow communication to/from server
EXPOSE 8000

RUN mkdir -p /usr/src/app
COPY . /usr/src/app

RUN apt-get install -y gcc libssl-dev nginx && conda config --add channels conda-forge && conda env create -f /usr/src/app/loadinsight-environment.yml


WORKDIR "/usr/src/app/backend"
# CMD specifcies the command to execute to start the server running.
CMD ["/usr/src/app/start.sh"]
# done!
