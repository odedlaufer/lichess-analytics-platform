#!/bin/bash
# Runs once, on an empty data volume. One database + owner role per service.
set -euo pipefail

create_db() {
  local db="$1" role="$2" password="$3"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
       -v db="$db" -v role="$role" -v password="$password" <<'SQL'
CREATE ROLE :"role" LOGIN PASSWORD :'password';
CREATE DATABASE :"db" OWNER :"role";
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
SQL
  echo "created database ${db} owned by ${role}"
}

create_db sqlmesh_state sqlmesh  "$SQLMESH_DB_PASSWORD"
create_db prefect       prefect  "$PREFECT_DB_PASSWORD"
create_db superset      superset "$SUPERSET_DB_PASSWORD"
