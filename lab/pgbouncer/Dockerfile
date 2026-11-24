FROM alpine:3.20

RUN apk add --no-cache pgbouncer python3 py3-psycopg2 ca-certificates && \
    adduser -D -u 10001 appuser && \
    mkdir -p /etc/pgbouncer /var/run/pgbouncer && \
    chown -R appuser:appuser /etc/pgbouncer /var/run/pgbouncer

COPY pgbouncer.ini /etc/pgbouncer/pgbouncer.ini.tmpl
COPY control.py /usr/local/bin/control.py
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 755 /usr/local/bin/entrypoint.sh /usr/local/bin/control.py && \
    chown appuser:appuser /etc/pgbouncer/pgbouncer.ini.tmpl

USER appuser
EXPOSE 6432 8091

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
