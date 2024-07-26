#!/bin/bash
set -e

# Start the original entrypoint script in the background
docker-entrypoint.sh postgres &

# Wait for PostgreSQL to start
until pg_isready -h localhost -U postgres; do
  sleep 1
done

# Function to create a database and import the corresponding SQL file
import_sql() {
  local db_name=$1
  local sql_file=$2

  echo "Checking if database '$db_name' exists..."
  if ! psql -U postgres -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
    echo "Database '$db_name' does not exist. Creating..."
    psql -v ON_ERROR_STOP=1 --username "postgres" --command "CREATE DATABASE \"$db_name\";"
  else
    echo "Database '$db_name' already exists. Skipping creation..."
  fi

  echo "Importing '$sql_file' into database '$db_name'..."
  psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "$db_name" -f "/sql_files/$sql_file"
}

# Import each backup file into its corresponding database
for sf in 0.01 0.05 1; do
    DB_NAME="tpch_sf${sf//./_}"
    import_sql "$DB_NAME" "backup_${DB_NAME}.sql"
done

# Wait for the original entrypoint script to finish
wait