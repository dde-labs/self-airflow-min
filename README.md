# Self: Airflow Minimal

This project implements **Airflow** for Production grade that does not use any 
Operator.

> [!NOTE]
> I will use Docker on WSL2 to provision the Airflow on my local machine.

## Prerequisite

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
