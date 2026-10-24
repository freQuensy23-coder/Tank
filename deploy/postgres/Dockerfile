FROM postgres:16-bookworm@sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4
COPY init.sh /docker-entrypoint-initdb.d/10-tank.sh
COPY pg_hba.conf /etc/postgresql/tank-pg_hba.conf
RUN chmod 755 /docker-entrypoint-initdb.d/10-tank.sh && chmod 644 /etc/postgresql/tank-pg_hba.conf
ENV POSTGRES_USER=postgres \
    POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD gosu postgres pg_isready -U postgres -d postgres
CMD ["postgres", \
     "-c", "password_encryption=scram-sha-256", \
     "-c", "hba_file=/etc/postgresql/tank-pg_hba.conf", \
     "-c", "max_locks_per_transaction=256", \
     "-c", "shared_buffers=4GB", \
     "-c", "effective_cache_size=12GB", \
     "-c", "work_mem=64MB", \
     "-c", "maintenance_work_mem=1GB", \
     "-c", "autovacuum_work_mem=512MB"]
