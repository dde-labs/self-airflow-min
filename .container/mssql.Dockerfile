# Ref: https://github.com/microsoft/mssql-docker/blob/master/linux/preview/examples/mssql-customize/
# FROM mcr.microsoft.com/mssql/server:2019-GA-ubuntu-22.04
FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends\
        gettext \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

RUN mkdir -p /usr/config

WORKDIR /usr/config

COPY ./scripts/mssql-init/configure-db.sh .
COPY ./scripts/mssql-init/entrypoint.sh .
COPY ./scripts/mssql-init/setup.sql .

RUN chmod +x /usr/config/entrypoint.sh

RUN chmod +x /usr/config/configure-db.sh

ENV MSSQL_USER "${MSSQL_USER}"
ENV MSSQL_PASS "${MSSQL_PASS}"
ENV MSSQL_DB "${MSSQL_DB}"

ENTRYPOINT ["./entrypoint.sh"]
