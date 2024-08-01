#!/bin/bash
sleep 60
# Wait 60 seconds for SQL Server to start up by ensuring that
# calling SQLCMD does not return an error code, which will ensure that sqlcmd is accessible
# and that system and user databases return "0" which means all databases are in an "online" state
# https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql?view=sql-server-2017

DB_STATUS=1
ERRCODE=1
i=0

while [[ $DB_STATUS -ne 0 ]] && [[ $i -lt 60 ]] && [[ $ERRCODE -ne 0 ]]; do
  i=$i+1
  DB_STATUS=$(/opt/mssql-tools18/bin/sqlcmd -h -1 -t 1 -U sa -P "${MSSQL_SA_PASS}" -C -Q "SET NOCOUNT ON; Select SUM(state) from sys.databases")
  ERRCODE=$?
  sleep 1
done

if [[ "$DB_STATUS" -ne 0 ]] || [[ "$ERRCODE" -ne 0 ]]; then
  echo "SQL Server took more than 60 seconds to start up or one or more databases are not in an ONLINE state"
  exit 1
fi

echo "Start Prepare setup.sql file ..."
envsubst < setup.sql > setup-out.sql && mv setup-out.sql setup.sql
cat setup.sql

/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASS}" -C -d master -i setup.sql
