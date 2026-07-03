#!/bin/sh
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || \
superset fab reset-password \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

superset init

superset set-database-uri \
  -d "${SUPERSET_TRINO_DATABASE_NAME:-Trino}" \
  -u "${SUPERSET_TRINO_SQLALCHEMY_URI:-trino://admin@trino:8080/iceberg}"
