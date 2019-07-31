FROM nginx:1.16.0-alpine

MAINTAINER Robert Metcalf

ENV APP_DIR /app

COPY ./app/ /${APP_DIR}
COPY ./keep_alive.sh /keep_alive.sh

RUN apk add --no-cache bash python3 curl python3-dev build-base linux-headers pcre-dev libffi-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache && \
    pip3 install --upgrade pip && \
    wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem -O /rds-combined-ca-bundle.pem && \
    pip3 install -r /${APP_DIR}/requirements.txt


CMD ["/keep_alive.sh"]

#docker build -t metcarob/object_store_abstraction_utility:0.0.2 .

#Running standalone
#docker run --rm -it metcarob/object_store_abstraction_utility:0.0.2 /app/run

#Running connected to store configured in secret
#docker service create --name object_store_abstraction_utility_serv --secret saas_user_management_system_objectstore_config_mig_to_dynamodb --secret saas_user_management_system_objectstore_config_dynamodb --secret saas_user_management_system_objectstore_config metcarob/object_store_abstraction_utility:0.0.2
#docker exec -it object_store_abstraction_utility_serv.1.XXXX /app/run
#docker service rm object_store_abstraction_utility_serv
