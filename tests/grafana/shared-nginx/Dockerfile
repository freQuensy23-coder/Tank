FROM nginx@sha256:165ddde8751e75a1c907d24ef10d9f6deb08b57e0fe56caf67512486aff76d82
RUN apt-get update && apt-get install -y --no-install-recommends python3 certbot curl openssl iproute2 && rm -rf /var/lib/apt/lists/*
