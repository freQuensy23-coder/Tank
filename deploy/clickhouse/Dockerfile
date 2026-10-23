FROM clickhouse/clickhouse-server:25.8@sha256:0152dd511befe6a2c2ef53e930726179669b08116da78500b37c51c96ff5ee77
COPY tank.xml /etc/clickhouse-server/config.d/tank.xml
COPY initdb/ /docker-entrypoint-initdb.d/
COPY tank-entrypoint.sh /tank-entrypoint.sh
RUN chmod 644 /etc/clickhouse-server/config.d/tank.xml && chmod 755 /tank-entrypoint.sh /docker-entrypoint-initdb.d/*.sh
ENV CLICKHOUSE_USER=tank \
    CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
HEALTHCHECK --interval=10s --timeout=5s --retries=6 --start-period=30s CMD clickhouse-client --user tank --password "$TANK_DB_PASSWORD" -q "SELECT 1"
ENTRYPOINT ["/tank-entrypoint.sh"]
