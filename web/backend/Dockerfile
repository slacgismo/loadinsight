
# Dockerfile

# FROM directive instructing base image to build upon
FROM python:3-onbuild

RUN set -ex \
    && RUN_DEPS=" \
        libpcre3 \
        mime-support \
        postgresql-client \
    " \
    && seq 1 8 | xargs -I{} mkdir -p /usr/share/man/man{} \
    && apt-get update && apt-get install -y --no-install-recommends $RUN_DEPS \
    && rm -rf /var/lib/apt/lists/*

# COPY startup script into known file location in container
COPY start.sh /usr/src/app/start.sh

# EXPOSE port 8000 to allow communication to/from server
EXPOSE 8000

WORKDIR "/usr/src/app/myproject"

# CMD specifcies the command to execute to start the server running.
CMD ["/usr/src/app/start.sh"]
# done!
