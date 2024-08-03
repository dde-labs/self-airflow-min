FROM apache/airflow:2.9.3-python3.11

USER root

COPY ./packages.txt /

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget \
        vim \
    && grep -vE '^\s*#' /packages.txt | sed -e 's/\r//g' | tr "\n" " " | xargs sudo apt-get install -y \
    && apt-get autoremove -yqq --purge \
    && apt-get clean

# NOTE: Add Microsoft repository before installing ODBC
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# NOTE: Install ODBC Driver version 17
RUN apt-get update  \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean

USER airflow

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

COPY --chown=airflow:root ../airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY --chown=airflow:root ./requirements.txt /

RUN uv pip install  \
    --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt
