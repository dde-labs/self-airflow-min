# Self: Airflow Minimal

Self Sandbox for create the best practice of Airflow with minimal generator DAG
concept.
I will implement it for the production grade that does not use any operator.

> [!NOTE]
> I will use Docker on WSL2 to provision the Airflow on my local machine.

## Prerequisite

### Environment Variables

`.env` file:

```text
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_PROJ_DIR=..
AIRFLOW_WEBSERVER_SECRET_KEY=<secret-key>
AIRFLOW_CORE_FARNET_KEY=<secret-key>

AIRFLOW_DB_CONN=postgresql+psycopg2://<user>:<password>@postgres/<database>
AIRFLOW_DB_USER=<user>
AIRFLOW_DB_PASS=<password>
AIRFLOW_DB_DB=<database>

MSSQL_SA_PASS=<password>
MSSQL_USER=<user>
MSSQL_DB=<database>
MSSQL_PASS=<password>
MSSQL_SCHEMA=<schema>
MSSQL_AIRFLOW_CONN='{
    "conn_type": "mssql",
    "login": "<user>",
    "password": "<password>",
    "host": "mssql",
    "port": 1433,
    "schema": "<schema>"
}'
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
