#!/bin/sh
set -eu

# Generate runtime config & userlist from environment variables (no secrets baked in image)
cp /etc/pgbouncer/pgbouncer.ini.tmpl /etc/pgbouncer/pgbouncer.ini
cat > /etc/pgbouncer/userlist.txt <<EOF
"appuser" "${APP_DB_PASSWORD}"
"migrator" "${SOURCE_MIGRATOR_PASSWORD}"
"pgbouncer_admin" "${PGBOUNCER_ADMIN_PASSWORD}"
EOF
chmod 600 /etc/pgbouncer/userlist.txt

pgbouncer /etc/pgbouncer/pgbouncer.ini &
sleep 1
exec python3 /usr/local/bin/control.py
