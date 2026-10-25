FROM sandbox
ARG IMAGE_VERSION
RUN test "$IMAGE_VERSION" = A -o "$IMAGE_VERSION" = B \
    && printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$IMAGE_VERSION" > /usr/local/bin/tank-upgrade-marker \
    && chmod 755 /usr/local/bin/tank-upgrade-marker \
    && printf 'VERSION = "%s"\n' "$IMAGE_VERSION" > /usr/local/lib/python3.12/site-packages/tank_qualification_version.py
