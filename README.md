# Self: Airflow Minimal

Self Sandbox for create the best practice of Airflow with minimal generator DAG
concept.
I will implement it for the production grade that does not use any operator.

> [!NOTE]
> I will use Docker on WSL2 to provision the Airflow on my local machine.

## Prerequisite

### Environment Variables

Create `.env` file for passing necessary environment variables to the Airflow
application.

```text
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_PROJ_DIR=..
AIRFLOW_WEBSERVER_SECRET_KEY=<generated-hash-string>
AIRFLOW_CORE_FARNET_KEY=<generated-hash-string>

AIRFLOW_DB_USER=<airflow-user>
AIRFLOW_DB_PASS=<airflow-pass>
AIRFLOW_DB_DB=airflow
AIRFLOW_DB_CONN=postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASS}@postgres/${AIRFLOW_DB_DB}
```

### Services

You should provision necessary Docker composes:

```shell
docker compose -f ./.container/docker-compose.warehouse.yml --env-file .env up -d
docker compose -f ./.container/docker-compose.yml --env-file .env up -d
```

> [!NOTE]
> Down Docker Compose that was provisioned from above;
> ```shell
> docker compose -f ./.container/docker-compose.yml --env-file .env down -v
> docker compose -f ./.container/docker-compose.warehouse.yml --env-file .env down
> ```

On the `mssql`, you can access this database with `sa` user.

## Project Structure

This project will design with less-dependency import structure and use scalable
strategy design.

```text
- /dags
    - /common
        - <common-process-dag>.py
        - ...
    - /conf
        - <config-template>.yaml
        - ...
    - /opt
        - <operation-dag>.py
        - ...
    - <manual-stream-dag>.py
    - ...
    - .airflowignore
- /logs
    - /airflow
        - ...
    - /internal
        - watermark.sqlite
        - logging.sqlite
- /plugins
    - /metadata
        - models.py
        - schemas.py
        - services.py
    - /operators
        - trigger.py
    - callback.py
    - db.py
    - utils.py
- /tests
- .env
- airflow.cfg
- packages.txt
- requirements.txt
```

## Getting Started

### Process Template

The template for the DAG generator that use for an input will store in the 
`./dags/conf` directory.

```yaml
stream_id: "<stream-name>"
process_groups:
    - priority: 1
      id: <process-group-name>
      processes:
        - priority: 1
          id: <process-name>
          ...
```
