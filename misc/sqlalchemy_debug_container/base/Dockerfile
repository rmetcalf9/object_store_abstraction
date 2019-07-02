FROM nginx:1.16.0-alpine

MAINTAINER Robert Metcalf

ENV APP_DIR /app

COPY ./run_app_docker.sh /run_app_docker.sh
COPY ./uwsgi.ini /uwsgi.ini
COPY ./nginx_default.conf /etc/nginx/conf.d/default.conf
COPY ./requirements.txt /requirements.txt

RUN apk add --no-cache bash python3 curl python3-dev build-base linux-headers pcre-dev libffi-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache && \
    pip3 install --upgrade pip && \
    mkdir /var/log/uwsgi && \
    mkdir ${APP_DIR} && \
    pip3 install uwsgi && \
    wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem -O /rds-combined-ca-bundle.pem && \
    pip3 install -r /requirements.txt


CMD ["/run_app_docker.sh"]

##docker rmi object_store_abstraction_misc_sqlachemy_debug_base
##docker build -t object_store_abstraction_misc_sqlachemy_debug_base .

#now run Dockerfile
