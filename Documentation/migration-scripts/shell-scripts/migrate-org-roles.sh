#!/usr/bin/env bash

# This script performs a multi-tenant database migration by detecting tenant tables,
# creating tenant-specific org_roles tables, and inserting user organization and
# platform role mappings while preventing duplicate entries. This script is specifically for mentoring.

# Load environment variables from migration.env
if [ -f "./migration.env" ]; then
    export $(grep -v '^#' ./migration.env | xargs)
else
    echo "migration.env file not found!"
    exit 1
fi

# Discover tenant codes by listing *_users tables
TENANTS=$(psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -t -c "
  SELECT replace(tablename, '_users', '')
  FROM pg_tables
  WHERE tablename LIKE '%_users'
  ORDER BY tablename;
")

for TENANT in $TENANTS; do
  TENANT=$(echo $TENANT | xargs) # trim spaces
  if [ -z "$TENANT" ]; then
    continue
  fi

  echo "Processing tenant: $TENANT"

  USER_META="${TENANT}_users_metadata"
  ORG_ROLE="${TENANT}_org_roles"

  # Ensure target table exists
  psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "
    CREATE TABLE IF NOT EXISTS \"$ORG_ROLE\" (
      id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      user_id INT,
      org_id INT,
      org_name TEXT,
      role_id INT,
      role_name TEXT,
      UNIQUE(user_id, org_id, role_id)
    );
  "

  # Insert org + role combinations, skip already existing rows
  psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" -c "
    INSERT INTO \"$ORG_ROLE\" (user_id, org_id, org_name, role_id, role_name)
    SELECT
      org.user_id,
      org.attribute_label::INT AS org_id,
      org.attribute_value AS org_name,
      role.attribute_label::INT AS role_id,
      role.attribute_value AS role_name
    FROM \"$USER_META\" org
    JOIN \"$USER_META\" role
      ON org.user_id = role.user_id
    WHERE org.attribute_code = 'Organizations'
      AND role.attribute_code = 'Platform Role'
      AND NOT EXISTS (
        SELECT 1 FROM \"$ORG_ROLE\" t
        WHERE t.user_id = org.user_id
          AND t.org_id = org.attribute_label::INT
          AND t.role_id = role.attribute_label::INT
      );
  "

  echo "✅ Migration completed for tenant: $TENANT"
done
