#!/bin/sh
set -eu
publisher_directory=$(dirname "$(readlink -f "$0")")
exec /usr/bin/env --default-signal=HUP,INT,TERM /bin/bash "$publisher_directory/publisher_worker.bash" "$@"
